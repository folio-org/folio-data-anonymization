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
public class LastNameAnonymization implements JobFactory {

  private static final List<FieldReference> LAST_NAME_FIELDS = List.of(
    new FieldReference("circulation_storage", "actual_cost_record", "jsonb", "$.user.lastName"),
    new FieldReference("circulation_storage", "request", "jsonb", "$.requester.lastName"),
    new FieldReference("circulation_storage", "request", "jsonb", "$.proxy.lastName"),
    new FieldReference("data_export", "job_executions", "run_by_last_name"),
    new FieldReference("data_export", "job_executions", "jsonb", "$.runBy.lastName"),
    new FieldReference("data_export", "job_profiles", "jsonb", "$.userInfo.lastName"),
    new FieldReference("data_export", "mapping_profiles", "jsonb", "$.userInfo.lastName"),
    new FieldReference("data_export", "job_profiles", "updated_by_last_name"),
    new FieldReference("data_export", "mapping_profiles", "updated_by_last_name"),
    new FieldReference("data_import", "default_file_extensions", "jsonb", "$.userInfo.lastName"),
    new FieldReference("data_import", "file_extensions", "jsonb", "$.userInfo.lastName"),
    new FieldReference("di_converter_storage", "action_profiles", "jsonb", "$.userInfo.lastName"),
    new FieldReference("di_converter_storage", "job_profiles", "jsonb", "$.userInfo.lastName"),
    new FieldReference("di_converter_storage", "mapping_profiles", "jsonb", "$.userInfo.lastName"),
    new FieldReference("di_converter_storage", "match_profiles", "jsonb", "$.userInfo.lastName"),
    new FieldReference("di_converter_storage", "profile_snapshots", "jsonb", "$.content.userInfo.lastName"),
    new FieldReference("inventory_storage", "item", "jsonb", "$.circulationNotes[*].source.personal.lastName"),
    new FieldReference("inventory_storage", "audit_item", "jsonb", "$.record.circulationNotes[*].source.lastName"),
    new FieldReference("oa", "party", "p_family_name"),
    new FieldReference("organizations_storage", "contacts", "jsonb", "$.lastName"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.contacts.lastName"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.privilegedContacts.lastName"),
    new FieldReference("organizations_storage", "privileged_contacts", "jsonb", "$.lastName"),
    new FieldReference("requests_mediated", "mediated_request", "requester_last_name"),
    new FieldReference("requests_mediated", "mediated_request", "proxy_last_name"),
    new FieldReference("search", "item", "json", "$.circulationNotes[*].source.personal.lastName"),
    new FieldReference(
      "source_record_manager",
      "job_execution",
      "job_profile_snapshot_wrapper",
      "$.content.userInfo.lastName"
    ),
    new FieldReference("source_record_manager", "job_execution", "job_user_last_name"),
    new FieldReference("users", "staging_users", "jsonb", "$.generalInfo.lastName"),
    new FieldReference("users", "users", "jsonb", "$.personal.lastName")
  );

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    List<JobConfigurationProperty> configuration = JobConfigurationProperty.fromFieldList(LAST_NAME_FIELDS, tenant);

    return List.of(
      new JobBuilder(
        "Last name anonymization",
        "Replaces last-name values with Faker-generated realistic-appearing values",
        tenant,
        context,
        configuration,
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
                        RandomValueUtils.lastNames(Math.max(end - start, 5))
                      )
                  )
                )
                .toList()
            )
      )
    );
  }
}
