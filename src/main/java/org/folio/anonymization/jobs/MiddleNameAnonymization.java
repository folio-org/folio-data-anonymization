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
import org.folio.anonymization.util.SystemUserExclusionUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MiddleNameAnonymization implements JobFactory {

  private static final List<FieldReference> MIDDLE_NAME_FIELDS = List.of(
    new FieldReference("circulation_storage", "actual_cost_record", "jsonb", "$.user.middleName"),
    new FieldReference("circulation_storage", "request", "jsonb", "$.requester.middleName"),
    new FieldReference("circulation_storage", "request", "jsonb", "$.proxy.middleName"),
    new FieldReference("requests_mediated", "mediated_request", "requester_middle_name"),
    new FieldReference("requests_mediated", "mediated_request", "proxy_middle_name"),
    new FieldReference("users", "staging_users", "jsonb", "$.generalInfo.middleName"),
    new FieldReference("users", "users", "jsonb", "$.personal.middleName")
  );

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "Middle name anonymization",
        "Replaces middle-name values with Faker-generated realistic-appearing values",
        tenant,
        context,
        JobConfigurationProperty.fromFieldList(MIDDLE_NAME_FIELDS, tenant),
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
                        RandomValueUtils.middleNames(Math.max(end - start, 5))
                      ),
                    SystemUserExclusionUtil.getExclusionCondition(field, tenant)
                  )
                )
                .toList()
            )
      )
    );
  }
}
