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
public class FirstNameAnonymization implements JobFactory {

  private static final FieldReference INVENTORY_ITEM_FIRST_NAME_FIELD = new FieldReference(
    "inventory_storage",
    "item",
    "jsonb",
    "$.circulationNotes[*].source.personal.firstName"
  );

  private static final List<FieldReference> FIRST_NAME_FIELDS = List.of(
    new FieldReference("circulation_storage", "actual_cost_record", "jsonb", "$.user.firstName"),
    new FieldReference("circulation_storage", "request", "jsonb", "$.requester.firstName"),
    new FieldReference("circulation_storage", "request", "jsonb", "$.proxy.firstName"),
    new FieldReference("data_export", "job_executions", "run_by_first_name"),
    new FieldReference("data_export", "job_executions", "jsonb", "$.runBy.firstName"),
    new FieldReference("data_export", "job_profiles", "jsonb", "$.userInfo.firstName"),
    new FieldReference("data_export", "mapping_profiles", "jsonb", "$.userInfo.firstName"),
    new FieldReference("data_export", "job_profiles", "updated_by_first_name"),
    new FieldReference("data_export", "mapping_profiles", "updated_by_first_name"),
    new FieldReference("data_import", "default_file_extensions", "jsonb", "$.userInfo.firstName"),
    new FieldReference("data_import", "file_extensions", "jsonb", "$.userInfo.firstName"),
    new FieldReference("di_converter_storage", "action_profiles", "jsonb", "$.userInfo.firstName"),
    new FieldReference("di_converter_storage", "job_profiles", "jsonb", "$.userInfo.firstName"),
    new FieldReference("di_converter_storage", "mapping_profiles", "jsonb", "$.userInfo.firstName"),
    new FieldReference("di_converter_storage", "match_profiles", "jsonb", "$.userInfo.firstName"),
    new FieldReference("di_converter_storage", "profile_snapshots", "jsonb", "$.content.userInfo.firstName"),
    new FieldReference("erm_usage", "usage_data_providers", "jsonb", "$.sushiCredentials.requestorName"),
    INVENTORY_ITEM_FIRST_NAME_FIELD,
    new FieldReference("inventory_storage", "audit_item", "jsonb", "$.record.circulationNotes[*].source.firstName"),
    new FieldReference("oa", "party", "p_given_names"),
    new FieldReference("organizations_storage", "contacts", "jsonb", "$.firstName"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.contacts.firstName"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.privilegedContacts.firstName"),
    new FieldReference("organizations_storage", "privileged_contacts", "jsonb", "$.firstName"),
    new FieldReference("requests_mediated", "mediated_request", "requester_first_name"),
    new FieldReference("requests_mediated", "mediated_request", "proxy_first_name"),
    new FieldReference("search", "item", "json", "$.circulationNotes[*].source.personal.firstName"),
    new FieldReference(
      "source_record_manager",
      "job_execution",
      "job_profile_snapshot_wrapper",
      "$.content.userInfo.firstName"
    ),
    new FieldReference("source_record_manager", "job_execution", "job_user_first_name"),
    new FieldReference("users", "staging_users", "jsonb", "$.generalInfo.firstName"),
    new FieldReference("users", "staging_users", "jsonb", "$.generalInfo.preferredFirstName"),
    new FieldReference("users", "users", "jsonb", "$.personal.firstName"),
    new FieldReference("users", "users", "jsonb", "$.personal.preferredFirstName")
  );

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    List<JobConfigurationProperty> configuration = JobConfigurationProperty.fromFieldList(FIRST_NAME_FIELDS, tenant);
    configuration
      .stream()
      .filter(property -> INVENTORY_ITEM_FIRST_NAME_FIELD.equals(property.getKey()))
      .forEach(property -> property.setBooleanValue(false));

    return List.of(
      new JobBuilder(
        "First name anonymization",
        "Replaces first-name values with Faker-generated realistic-appearing values",
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
                        RandomValueUtils.firstNames(Math.max(end - start, 5))
                      )
                  )
                )
                .toList()
            )
      )
    );
  }
}
