package org.folio.anonymization.jobs;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.inline;

import java.util.List;
import org.folio.anonymization.config.JobConfig;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.BatchGenerationFromTablePart;
import org.folio.anonymization.jobs.templates.ReplaceJSONBValuePart;
import org.jooq.JSONB;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class UserCredentialsAnonymization implements JobFactory {

  private static final String REPLACE_AUTH_CREDENTIALS = "replace-auth-credentials";
  private static final String REPLACE_AUTH_CREDENTIALS_HISTORY = "replace-auth-credentials-history";
  private static final String ANONYMIZED_SALT = "5DA98A050D72C4E0A914B2858EAA2A18B9264D01";
  private static final String ANONYMIZED_HASH = "56B87EAD16C0D4D22DD7612728FEB12A200D1485";
  private static final FieldReference AUTH_CREDENTIALS_ID = new FieldReference("login", "auth_credentials", "id");
  private static final FieldReference AUTH_CREDENTIALS_HISTORY_ID = new FieldReference(
    "login",
    "auth_credentials_history",
    "id"
  );

  private static final FieldReference AUTH_CREDENTIALS_HASH = new FieldReference(
    "login",
    "auth_credentials",
    "jsonb",
    "$.hash"
  );
  private static final FieldReference AUTH_CREDENTIALS_SALT = new FieldReference(
    "login",
    "auth_credentials",
    "jsonb",
    "$.salt"
  );
  private static final FieldReference AUTH_CREDENTIALS_HISTORY_HASH = new FieldReference(
    "login",
    "auth_credentials_history",
    "jsonb",
    "$.hash"
  );
  private static final FieldReference AUTH_CREDENTIALS_HISTORY_SALT = new FieldReference(
    "login",
    "auth_credentials_history",
    "jsonb",
    "$.salt"
  );

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    boolean hasAuthCredentials = hasTable(tenant, "login", "auth_credentials");
    boolean hasAuthCredentialsHistory = hasTable(tenant, "login", "auth_credentials_history");

    List<JobConfigurationProperty> configuration = List.of(
      new JobConfigurationProperty(
        REPLACE_AUTH_CREDENTIALS,
        "Replace mod_login.auth_credentials hash and salt with anonymized constants",
        true,
        !hasAuthCredentials
      ),
      new JobConfigurationProperty(
        REPLACE_AUTH_CREDENTIALS_HISTORY,
        "Replace mod_login.auth_credentials_history hash and salt with anonymized constants",
        true,
        !hasAuthCredentialsHistory
      )
    );

    return List.of(
      new JobBuilder(
        "User credentials anonymization",
        "Replaces login credential hash/salt values with fixed anonymized constants.",
        tenant,
        context,
        configuration,
        ctx -> {
          Job job = new Job(ctx, List.of("prepare", "overwrite"));

          if (JobConfigurationProperty.isOn(ctx.settings(), REPLACE_AUTH_CREDENTIALS)) {
            job.scheduleParts(
              "prepare",
              List.of(
                new BatchGenerationFromTablePart<>(
                  "Prepare to replace login.auth_credentials.jsonb->'$.hash'",
                  AUTH_CREDENTIALS_ID,
                  Object.class,
                  JobConfig.BATCH_SIZE,
                  "overwrite",
                  (label, condition, start, end) ->
                    new ReplaceJSONBValuePart(
                      "Set login.auth_credentials.jsonb->'$.hash' to anonymized constant on " + label,
                      AUTH_CREDENTIALS_HASH,
                      condition,
                      field("to_jsonb(cast({0} as text))", JSONB.class, inline(ANONYMIZED_HASH))
                    )
                ),
                new BatchGenerationFromTablePart<>(
                  "Prepare to replace login.auth_credentials.jsonb->'$.salt'",
                  AUTH_CREDENTIALS_ID,
                  Object.class,
                  JobConfig.BATCH_SIZE,
                  "overwrite",
                  (label, condition, start, end) ->
                    new ReplaceJSONBValuePart(
                      "Set login.auth_credentials.jsonb->'$.salt' to anonymized constant on " + label,
                      AUTH_CREDENTIALS_SALT,
                      condition,
                      field("to_jsonb(cast({0} as text))", JSONB.class, inline(ANONYMIZED_SALT))
                    )
                )
              )
            );
          }

          if (JobConfigurationProperty.isOn(ctx.settings(), REPLACE_AUTH_CREDENTIALS_HISTORY)) {
            job.scheduleParts(
              "prepare",
              List.of(
                new BatchGenerationFromTablePart<>(
                  "Prepare to replace login.auth_credentials_history.jsonb->'$.hash'",
                  AUTH_CREDENTIALS_HISTORY_ID,
                  Object.class,
                  JobConfig.BATCH_SIZE,
                  "overwrite",
                  (label, condition, start, end) ->
                    new ReplaceJSONBValuePart(
                      "Set login.auth_credentials_history.jsonb->'$.hash' to anonymized constant on " + label,
                      AUTH_CREDENTIALS_HISTORY_HASH,
                      condition,
                      field("to_jsonb(cast({0} as text))", JSONB.class, inline(ANONYMIZED_HASH))
                    )
                ),
                new BatchGenerationFromTablePart<>(
                  "Prepare to replace login.auth_credentials_history.jsonb->'$.salt'",
                  AUTH_CREDENTIALS_HISTORY_ID,
                  Object.class,
                  JobConfig.BATCH_SIZE,
                  "overwrite",
                  (label, condition, start, end) ->
                    new ReplaceJSONBValuePart(
                      "Set login.auth_credentials_history.jsonb->'$.salt' to anonymized constant on " + label,
                      AUTH_CREDENTIALS_HISTORY_SALT,
                      condition,
                      field("to_jsonb(cast({0} as text))", JSONB.class, inline(ANONYMIZED_SALT))
                    )
                )
              )
            );
          }

          return job;
        }
      )
    );
  }

  private static boolean hasTable(TenantExecutionContext tenant, String schema, String table) {
    return tenant
      .availableTables()
      .stream()
      .anyMatch(candidate -> schema.equals(candidate.schema()) && table.equals(candidate.table()));
  }
}
