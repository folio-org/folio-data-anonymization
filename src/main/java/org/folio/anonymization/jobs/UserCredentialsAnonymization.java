package org.folio.anonymization.jobs;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.inline;
import static org.jooq.impl.DSL.trueCondition;

import java.util.ArrayList;
import java.util.List;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.JobPart;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
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
          Job job = new Job(ctx, List.of("overwrite"));
          List<JobPart> parts = new ArrayList<>();

          if (JobConfigurationProperty.isOn(ctx.settings(), REPLACE_AUTH_CREDENTIALS)) {
            parts.add(
              new ReplaceJSONBValuePart(
                "Set login.auth_credentials.jsonb->'$.hash' to anonymized constant",
                AUTH_CREDENTIALS_HASH,
                trueCondition(),
                field("to_jsonb(cast({0} as text))", JSONB.class, inline(ANONYMIZED_HASH))
              )
            );
            parts.add(
              new ReplaceJSONBValuePart(
                "Set login.auth_credentials.jsonb->'$.salt' to anonymized constant",
                AUTH_CREDENTIALS_SALT,
                trueCondition(),
                field("to_jsonb(cast({0} as text))", JSONB.class, inline(ANONYMIZED_SALT))
              )
            );
          }

          if (JobConfigurationProperty.isOn(ctx.settings(), REPLACE_AUTH_CREDENTIALS_HISTORY)) {
            parts.add(
              new ReplaceJSONBValuePart(
                "Set login.auth_credentials_history.jsonb->'$.hash' to anonymized constant",
                AUTH_CREDENTIALS_HISTORY_HASH,
                trueCondition(),
                field("to_jsonb(cast({0} as text))", JSONB.class, inline(ANONYMIZED_HASH))
              )
            );
            parts.add(
              new ReplaceJSONBValuePart(
                "Set login.auth_credentials_history.jsonb->'$.salt' to anonymized constant",
                AUTH_CREDENTIALS_HISTORY_SALT,
                trueCondition(),
                field("to_jsonb(cast({0} as text))", JSONB.class, inline(ANONYMIZED_SALT))
              )
            );
          }

          job.scheduleParts("overwrite", parts);
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
