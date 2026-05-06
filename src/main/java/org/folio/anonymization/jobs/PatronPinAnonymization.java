package org.folio.anonymization.jobs;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.inline;
import static org.jooq.impl.DSL.trueCondition;

import java.util.List;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.ReplaceJSONBValuePart;
import org.jooq.JSONB;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PatronPinAnonymization implements JobFactory {

  private static final String REPLACE_PATRON_PIN = "replace-patron-pin";
  private static final String ANONYMIZED_PIN = "0000";
  private static final FieldReference PATRON_PIN_FIELD = new FieldReference("users", "patronpin", "jsonb", "$.pin");

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    boolean hasPatronPinTable = hasTable(tenant, "users", "patronpin");

    List<JobConfigurationProperty> configuration = List.of(
      new JobConfigurationProperty(
        REPLACE_PATRON_PIN,
        "Replace mod_users.patronpin $.pin with a 4-digit anonymized value",
        true,
        !hasPatronPinTable
      )
    );

    return List.of(
      new JobBuilder(
        "Patron PIN anonymization",
        "Replaces patron PIN values with a fixed 4-digit anonymized value.",
        tenant,
        context,
        configuration,
        ctx ->
          new Job(ctx, List.of("overwrite"))
            .scheduleParts(
              "overwrite",
              JobConfigurationProperty.isOn(ctx.settings(), REPLACE_PATRON_PIN)
                ? List.of(
                  new ReplaceJSONBValuePart(
                    "Set users.patronpin.jsonb->'$.pin' to anonymized value",
                    PATRON_PIN_FIELD,
                    trueCondition(),
                    field("to_jsonb(cast({0} as text))", JSONB.class, inline(ANONYMIZED_PIN))
                  )
                )
                : List.of()
            )
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
