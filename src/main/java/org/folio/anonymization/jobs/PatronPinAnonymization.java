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
public class PatronPinAnonymization implements JobFactory {

  private static final String ANONYMIZED_PIN = "0000";
  private static final FieldReference PATRON_PIN_FIELD = new FieldReference("users", "patronpin", "jsonb", "$.pin");

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "Patron PIN anonymization",
        "Replaces patron PINs with '0000'.",
        tenant,
        context,
        JobConfigurationProperty.fromFieldList(List.of(PATRON_PIN_FIELD), tenant),
        ctx ->
          new Job(ctx, List.of("prepare", "overwrite"))
            .scheduleParts(
              "prepare",
              JobConfigurationProperty
                .getEnabledFields(ctx.settings())
                .map(targetField ->
                  new BatchGenerationFromTablePart<>(
                    "Prepare to redact " + targetField.toString(),
                    targetField,
                    JobConfig.BATCH_SIZE,
                    "overwrite",
                    (label, condition, start, end) ->
                      new ReplaceJSONBValuePart(
                        "Replace " + targetField.toString() + " on " + label,
                        targetField,
                        condition,
                        field("to_jsonb(cast({0} as text))", JSONB.class, inline(ANONYMIZED_PIN))
                      )
                  )
                )
                .toList()
            )
      )
    );
  }
}
