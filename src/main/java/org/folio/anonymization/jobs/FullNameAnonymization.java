package org.folio.anonymization.jobs;

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
import org.folio.anonymization.jobs.templates.ReplaceValueFromListPart;
import org.folio.anonymization.util.RandomValueUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class FullNameAnonymization implements JobFactory {

  private static final List<FieldReference> FULL_NAME_FIELDS = List.of(
    new FieldReference("courses", "coursereserves_courselistings", "jsonb", "$.instructorObjects[*].name"),
    new FieldReference("courses", "coursereserves_instructors", "jsonb", "$.name"),
    new FieldReference("feesfines", "feefineactions", "jsonb", "$.source"),
    new FieldReference("inn_reach", "transaction_hold", "patron_name"),
    new FieldReference("lists", "list_details", "created_by_username"),
    new FieldReference("lists", "list_details", "updated_by_username"),
    new FieldReference("lists", "list_refresh_details", "refreshed_by_username"),
    new FieldReference("lists", "list_versions", "updated_by_username"),
    new FieldReference("oa", "party", "p_full_name"),
    new FieldReference("orders_storage", "po_line", "jsonb", "$.requester"),
    new FieldReference("orders_storage", "po_line", "jsonb", "$.donor"),
    new FieldReference("orders_storage", "po_line", "jsonb", "$.selector")
  );

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "Full name anonymization",
        "Replaces full-name values with Faker-generated realistic-appearing values",
        tenant,
        context,
        JobConfigurationProperty.fromFieldList(FULL_NAME_FIELDS, tenant),
        ctx ->
          new Job(ctx, List.of("prepare", "overwrite"))
            .scheduleParts(
              "prepare",
              JobConfigurationProperty
                .getEnabledFields(ctx.settings())
                .map(field ->
                  new BatchGenerationFromTablePart<>(
                    "Prep to apply new values to " + field.toString(),
                    field,
                    JobConfig.BATCH_SIZE,
                    "overwrite",
                    (label, condition, start, end) ->
                      new ReplaceValueFromListPart(
                        "replace %s on %s".formatted(field.toString(), label),
                        field,
                        condition,
                        RandomValueUtils.fullNames(Math.max(end - start, 5))
                      )
                  )
                )
                .toList()
            )
      )
    );
  }
}
