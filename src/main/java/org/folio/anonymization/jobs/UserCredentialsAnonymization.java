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
import org.folio.anonymization.jobs.templates.ReplaceValuePart;
import org.jooq.JSONB;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class UserCredentialsAnonymization implements JobFactory {

  private static final String CLEAR_AUTH_CREDENTIALS = "clear-auth-credentials";
  private static final String CLEAR_AUTH_CREDENTIALS_HISTORY = "clear-auth-credentials-history";
  private static final String REPLACE_PATRON_PIN = "replace-patron-pin";

  private static final String ANONYMIZED_PIN = "0000";

  private static final FieldReference AUTH_CREDENTIALS_HASH = new FieldReference("login", "auth_credentials", "hash");
  private static final FieldReference AUTH_CREDENTIALS_SALT = new FieldReference("login", "auth_credentials", "salt");
  private static final FieldReference AUTH_CREDENTIALS_HISTORY_HASH = new FieldReference(
    "login",
    "auth_credentials_history",
    "hash"
  );
  private static final FieldReference AUTH_CREDENTIALS_HISTORY_SALT = new FieldReference(
    "login",
    "auth_credentials_history",
    "salt"
  );
  private static final FieldReference PATRON_PIN_FIELD = new FieldReference("users", "patronpin", "jsonb", "$.pin");

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    boolean hasAuthCredentials = hasTable(tenant, "login", "auth_credentials");
    boolean hasAuthCredentialsHistory = hasTable(tenant, "login", "auth_credentials_history");
    boolean hasPatronPinTable = hasTable(tenant, "users", "patronpin");

    List<JobConfigurationProperty> configuration = List.of(
      new JobConfigurationProperty(
        CLEAR_AUTH_CREDENTIALS,
        "Remove mod_login.auth_credentials hash and salt values",
        true,
        !hasAuthCredentials
      ),
      new JobConfigurationProperty(
        CLEAR_AUTH_CREDENTIALS_HISTORY,
        "Remove mod_login.auth_credentials_history hash and salt values",
        true,
        !hasAuthCredentialsHistory
      ),
      new JobConfigurationProperty(
        REPLACE_PATRON_PIN,
        "Replace mod_users.patronpin $.pin with a 4-digit anonymized value",
        true,
        !hasPatronPinTable
      )
    );

    return List.of(
      new JobBuilder(
        "User credentials anonymization",
        "Removes login credential hash/salt values and replaces patron PIN with a fixed 4-digit anonymized value.",
        tenant,
        context,
        configuration,
        ctx -> {
          Job job = new Job(ctx, List.of("overwrite"));
          List<JobPart> parts = new ArrayList<>();

          if (JobConfigurationProperty.isOn(ctx.settings(), CLEAR_AUTH_CREDENTIALS)) {
            parts.add(
              new ReplaceValuePart(
                "Clear login.auth_credentials.hash",
                AUTH_CREDENTIALS_HASH,
                trueCondition(),
                field("null")
              )
            );
            parts.add(
              new ReplaceValuePart(
                "Clear login.auth_credentials.salt",
                AUTH_CREDENTIALS_SALT,
                trueCondition(),
                field("null")
              )
            );
          }

          if (JobConfigurationProperty.isOn(ctx.settings(), CLEAR_AUTH_CREDENTIALS_HISTORY)) {
            parts.add(
              new ReplaceValuePart(
                "Clear login.auth_credentials_history.hash",
                AUTH_CREDENTIALS_HISTORY_HASH,
                trueCondition(),
                field("null")
              )
            );
            parts.add(
              new ReplaceValuePart(
                "Clear login.auth_credentials_history.salt",
                AUTH_CREDENTIALS_HISTORY_SALT,
                trueCondition(),
                field("null")
              )
            );
          }

          if (JobConfigurationProperty.isOn(ctx.settings(), REPLACE_PATRON_PIN)) {
            parts.add(
              new ReplaceJSONBValuePart(
                "Set users.patronpin.jsonb->'$.pin' to anonymized value",
                PATRON_PIN_FIELD,
                PATRON_PIN_FIELD.field(ctx.tenant().tenant(), String.class).isNotNull(),
                field("to_jsonb(cast({0} as text))", JSONB.class, inline(ANONYMIZED_PIN))
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
