package org.folio.anonymization.jobs;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.primaryKey;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.DSL.unique;

import java.util.List;
import java.util.stream.Stream;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.BatchGenerationFromSequencePart;
import org.folio.anonymization.jobs.templates.BatchGenerationFromTablePart;
import org.folio.anonymization.jobs.templates.CreateTablePart;
import org.folio.anonymization.jobs.templates.DropTablePart;
import org.folio.anonymization.jobs.templates.GenerateValuesPart;
import org.folio.anonymization.jobs.templates.InsertIntoTablePart;
import org.folio.anonymization.jobs.templates.ReplaceJSONBValuePart;
import org.folio.anonymization.jobs.templates.ReplaceValuePart;
import org.folio.anonymization.util.DBUtils;
import org.folio.anonymization.util.RandomValueUtils;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Table;
import org.jooq.impl.SQLDataType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class UsernameAnonymization implements JobFactory {

  private static final int BATCH_SIZE = 2_000;

  private static final List<FieldReference> FIELDS = List.of(
    new FieldReference("circulation_storage", "print_events", "jsonb", "$.requesterName"),
    new FieldReference("consortia_keycloak", "inactive_user_tenant", "username"),
    new FieldReference("consortia_keycloak", "user_tenant", "username"),
    new FieldReference("data_export", "job_profiles", "jsonb", "$.userInfo.userName"),
    new FieldReference("data_export", "mapping_profiles", "jsonb", "$.userInfo.userName"),
    new FieldReference("data_export", "job_profiles", "jsonb", "$.metadata.createdByUsername"),
    new FieldReference("data_export", "job_profiles", "jsonb", "$.metadata.updatedByUsername"),
    new FieldReference("data_export", "mapping_profiles", "jsonb", "$.metadata.createdByUsername"),
    new FieldReference("data_export", "mapping_profiles", "jsonb", "$.metadata.updatedByUsername"),
    new FieldReference("data_export_spring", "job", "created_by_username"),
    new FieldReference("data_export_spring", "job", "updated_by_username"),
    new FieldReference("data_import", "default_file_extensions", "jsonb", "$.userInfo.userName"),
    new FieldReference("data_import", "file_extensions", "jsonb", "$.userInfo.userName"),
    new FieldReference("data_import", "default_file_extensions", "jsonb", "$.metadata.createdByUsername"),
    new FieldReference("data_import", "default_file_extensions", "jsonb", "$.metadata.updatedByUsername"),
    new FieldReference("data_import", "file_extensions", "jsonb", "$.metadata.createdByUsername"),
    new FieldReference("data_import", "file_extensions", "jsonb", "$.metadata.updatedByUsername"),
    new FieldReference("data_import", "upload_definitions", "jsonb", "$.metadata.createdByUsername"),
    new FieldReference("data_import", "upload_definitions", "jsonb", "$.metadata.updatedByUsername"),
    new FieldReference("di_converter_storage", "action_profiles", "jsonb", "$.userInfo.userName"),
    new FieldReference("di_converter_storage", "job_profiles", "jsonb", "$.userInfo.userName"),
    new FieldReference("di_converter_storage", "mapping_profiles", "jsonb", "$.userInfo.userName"),
    new FieldReference("di_converter_storage", "match_profiles", "jsonb", "$.userInfo.userName"),
    new FieldReference("di_converter_storage", "profile_snapshots", "jsonb", "$.content.metadata.createdByUsername"),
    new FieldReference("di_converter_storage", "profile_snapshots", "jsonb", "$.content.metadata.updatedByUsername"),
    new FieldReference("di_converter_storage", "profile_snapshots", "jsonb", "$.content.userInfo.userName"),
    new FieldReference(
      "di_converter_storage",
      "marc_field_protection_settings",
      "jsonb",
      "$.metadata.createdByUsername"
    ),
    new FieldReference(
      "di_converter_storage",
      "marc_field_protection_settings",
      "jsonb",
      "$.metadata.updatedByUsername"
    ),
    new FieldReference("inn_reach", "agency_location_ac_mapping", "created_by_username"),
    new FieldReference("inn_reach", "agency_location_ac_mapping", "updated_by_username"),
    new FieldReference("inn_reach", "agency_location_lsc_mapping", "created_by_username"),
    new FieldReference("inn_reach", "agency_location_lsc_mapping", "updated_by_username"),
    new FieldReference("inn_reach", "agency_location_location_mapping", "created_by_username"),
    new FieldReference("inn_reach", "agency_location_location_mapping", "updated_by_username"),
    new FieldReference("inn_reach", "agency_location_mapping", "created_by_username"),
    new FieldReference("inn_reach", "agency_location_mapping", "updated_by_username"),
    new FieldReference("inn_reach", "central_patron_type_mapping", "created_by_username"),
    new FieldReference("inn_reach", "central_patron_type_mapping", "updated_by_username"),
    new FieldReference("inn_reach", "central_server", "created_by_username"),
    new FieldReference("inn_reach", "central_server", "updated_by_username"),
    new FieldReference("inn_reach", "contribution_criteria_configuration", "created_by_username"),
    new FieldReference("inn_reach", "contribution_criteria_configuration", "updated_by_username"),
    new FieldReference("inn_reach", "contribution", "created_by_username"),
    new FieldReference("inn_reach", "contribution", "updated_by_username"),
    new FieldReference("inn_reach", "inn_reach_location", "created_by_username"),
    new FieldReference("inn_reach", "inn_reach_location", "updated_by_username"),
    new FieldReference("inn_reach", "inn_reach_recall_user", "created_by_username"),
    new FieldReference("inn_reach", "inn_reach_recall_user", "updated_by_username"),
    new FieldReference("inn_reach", "inn_reach_transaction", "created_by_username"),
    new FieldReference("inn_reach", "inn_reach_transaction", "updated_by_username"),
    new FieldReference("inn_reach", "item_contribution_options_configuration", "created_by_username"),
    new FieldReference("inn_reach", "item_contribution_options_configuration", "updated_by_username"),
    new FieldReference("inn_reach", "item_type_mapping", "created_by_username"),
    new FieldReference("inn_reach", "item_type_mapping", "updated_by_username"),
    new FieldReference("inn_reach", "job_execution_status", "created_by_username"),
    new FieldReference("inn_reach", "job_execution_status", "updated_by_username"),
    new FieldReference("inn_reach", "library_mapping", "created_by_username"),
    new FieldReference("inn_reach", "library_mapping", "updated_by_username"),
    new FieldReference("inn_reach", "location_mapping", "created_by_username"),
    new FieldReference("inn_reach", "location_mapping", "updated_by_username"),
    new FieldReference("inn_reach", "marc_field_configuration", "created_by_username"),
    new FieldReference("inn_reach", "marc_field_configuration", "updated_by_username"),
    new FieldReference("inn_reach", "marc_transformation_options_settings", "created_by_username"),
    new FieldReference("inn_reach", "marc_transformation_options_settings", "updated_by_username"),
    new FieldReference("inn_reach", "material_type_mapping", "created_by_username"),
    new FieldReference("inn_reach", "material_type_mapping", "updated_by_username"),
    new FieldReference("inn_reach", "ongoing_contribution_status", "created_by_username"),
    new FieldReference("inn_reach", "ongoing_contribution_status", "updated_by_username"),
    new FieldReference("inn_reach", "paging_slip_template", "created_by_username"),
    new FieldReference("inn_reach", "paging_slip_template", "updated_by_username"),
    new FieldReference("inn_reach", "patron_type_mapping", "created_by_username"),
    new FieldReference("inn_reach", "patron_type_mapping", "updated_by_username"),
    new FieldReference("inn_reach", "transaction_hold", "created_by_username"),
    new FieldReference("inn_reach", "transaction_hold", "updated_by_username"),
    new FieldReference("inn_reach", "transaction_pickup_location", "created_by_username"),
    new FieldReference("inn_reach", "transaction_pickup_location", "updated_by_username"),
    new FieldReference("inn_reach", "user_custom_field_mapping", "created_by_username"),
    new FieldReference("inn_reach", "user_custom_field_mapping", "updated_by_username"),
    new FieldReference("inn_reach", "visible_patron_field_config", "created_by_username"),
    new FieldReference("inn_reach", "visible_patron_field_config", "updated_by_username"),
    new FieldReference("kb_ebsco_java", "kb_credentials", "created_by_user_name"),
    new FieldReference("kb_ebsco_java", "kb_credentials", "updated_by_user_name"),
    new FieldReference("kb_ebsco_java", "usage_consolidation_settings", "created_by_user_name"),
    new FieldReference("kb_ebsco_java", "usage_consolidation_settings", "updated_by_user_name"),
    new FieldReference("password_validator", "validationrules", "updated_by_username"),
    new FieldReference("password_validator", "validationrules", "created_by_username"),
    new FieldReference("remote_storage", "remote_storage_configurations", "updated_by_username"),
    new FieldReference("remote_storage", "remote_storage_configurations", "created_by_username"),
    new FieldReference("remote_storage", "retrieval_queue", "patron_name"),
    new FieldReference("requests_mediated", "batch_request", "created_by_username"),
    new FieldReference("requests_mediated", "batch_request", "updated_by_username"),
    new FieldReference("requests_mediated", "batch_request_split", "created_by_username"),
    new FieldReference("requests_mediated", "batch_request_split", "updated_by_username"),
    new FieldReference("requests_mediated", "mediated_request", "created_by_username"),
    new FieldReference("requests_mediated", "mediated_request", "updated_by_username"),
    new FieldReference(
      "source_record_manager",
      "job_execution",
      "job_profile_snapshot_wrapper",
      "$.content.userInfo.userName"
    ),
    new FieldReference("users", "addresstype", "jsonb", "$.metadata.createdByUsername"),
    new FieldReference("users", "addresstype", "jsonb", "$.metadata.updatedByUsername"),
    new FieldReference("users", "configuration", "jsonb", "$.metadata.createdByUsername"),
    new FieldReference("users", "configuration", "jsonb", "$.metadata.updatedByUsername"),
    new FieldReference("users", "custom_fields", "jsonb", "$.metadata.createdByUsername"),
    new FieldReference("users", "custom_fields", "jsonb", "$.metadata.updatedByUsername"),
    new FieldReference("users", "departments", "jsonb", "$.metadata.createdByUsername"),
    new FieldReference("users", "departments", "jsonb", "$.metadata.updatedByUsername"),
    new FieldReference("users", "groups", "jsonb", "$.metadata.createdByUsername"),
    new FieldReference("users", "groups", "jsonb", "$.metadata.updatedByUsername"),
    new FieldReference("users", "proxyfor", "jsonb", "$.metadata.createdByUsername"),
    new FieldReference("users", "proxyfor", "jsonb", "$.metadata.updatedByUsername"),
    new FieldReference("users", "staging_users", "jsonb", "$.metadata.createdByUsername"),
    new FieldReference("users", "staging_users", "jsonb", "$.metadata.updatedByUsername"),
    new FieldReference("users", "user_tenant", "username"),
    new FieldReference("users", "users", "jsonb", "$.username"),
    new FieldReference("users", "users", "jsonb", "$.metadata.createdByUsername"),
    new FieldReference("users", "users", "jsonb", "$.metadata.updatedByUsername")
  );

  @Autowired
  private SharedExecutionContext context;

  @SuppressWarnings("unchecked")
  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "Username anonymization",
        "Replaces usernames with unique anonymized values, ensuring integrity between tables.",
        tenant,
        context,
        Stream
          .concat(
            Stream.of(
              new JobConfigurationProperty(
                "create-table",
                "Create temporary table with current and new values (disable if resuming a previous run)"
              ),
              new JobConfigurationProperty(
                "drop-table",
                "Destroy temporary table (PII will be left behind if disabled)"
              )
            ),
            JobConfigurationProperty.fromFieldList(FIELDS, tenant).stream()
          )
          .toList(),
        ctx -> {
          // final contains the full [original,reference,newValue]
          // staging contains only [original,reference]
          // this allows us to copy from one to the other as we generate unique values
          Table<?> tempTableFinal = table(name("public", "_danon_" + ctx.tenant().tenant().id() + "_usernames"));
          Table<?> tempTableStaging = table(
            name("public", "_danon_" + ctx.tenant().tenant().id() + "_usernames_staging")
          );

          Field<String> originalValue = field("original_value", SQLDataType.VARCHAR.notNull());
          Field<String> newValue = field("new_value", SQLDataType.VARCHAR.null_());

          Job job = new Job(
            ctx,
            List.of(
              "prepare",
              "enumerate-prep",
              "enumerate",
              "generate-new-values-prep",
              "generate-new-values",
              "apply-new-values-prep",
              "apply-new-values",
              "cleanup"
            )
          );

          if (JobConfigurationProperty.isOn(ctx.settings(), "create-table")) {
            job.scheduleParts(
              "prepare",
              List.of(
                new CreateTablePart(
                  "Create temporary table (staging)",
                  tempTableStaging,
                  List.of(originalValue),
                  List.of(primaryKey(originalValue)),
                  true
                ),
                new CreateTablePart(
                  "Create temporary table (final)",
                  tempTableFinal,
                  List.of(originalValue, newValue),
                  List.of(primaryKey(originalValue), unique(newValue)),
                  false
                )
              )
            );
            job.scheduleParts(
              "enumerate-prep",
              JobConfigurationProperty
                .getEnabledFields(ctx.settings())
                .map(field -> {
                  return new BatchGenerationFromTablePart<>(
                    "Make batches to enumerate data from " + field.toString(),
                    field,
                    BATCH_SIZE,
                    "enumerate",
                    (label, condition, start, end) ->
                      new InsertIntoTablePart(
                        "Enumerate data from " + field.toString() + " " + label,
                        tempTableStaging,
                        select(field("a"))
                          .from(
                            select(field.field(ctx.tenant().tenant(), String.class).as("a"))
                              .from(field.table(ctx.tenant().tenant()))
                              .where(condition)
                          )
                          .where(field("a").isNotNull())
                      )
                  );
                })
                .toList()
            );

            job.scheduleParts(
              "generate-new-values-prep",
              List.of(
                new BatchGenerationFromSequencePart(
                  "Analyze table size for split processing",
                  tempTableStaging,
                  BATCH_SIZE,
                  "generate-new-values",
                  (label, cond, start, end) ->
                    new GenerateValuesPart(
                      "Generate values %s".formatted(label),
                      tempTableFinal,
                      newValue,
                      select(originalValue).from(tempTableStaging).where(cond),
                      RandomValueUtils.usernameGenerator(start)
                    )
                )
              )
            );
          }

          job.scheduleParts(
            "apply-new-values-prep",
            JobConfigurationProperty
              .getEnabledFields(ctx.settings())
              .map(field ->
                new BatchGenerationFromTablePart<>(
                  "Prep to apply new values to " + field.toString(),
                  field,
                  BATCH_SIZE,
                  "apply-new-values",
                  (label, condition, start, end) -> {
                    if (field.jsonPath() != null) {
                      return new ReplaceJSONBValuePart(
                        "replace %s on %s".formatted(field.toString(), label),
                        field,
                        innerField ->
                          field(
                            "to_jsonb(({0}))",
                            JSONB.class,
                            select(newValue)
                              .from(tempTableFinal)
                              .where(originalValue.eq(DBUtils.jsonbToString(innerField)))
                          ),
                        condition
                      );
                    } else {
                      return new ReplaceValuePart(
                        "replace %s on %s".formatted(field.toString(), label),
                        field,
                        innerField ->
                          field(
                            select(newValue).from(tempTableFinal).where(originalValue.eq((Field<String>) innerField))
                          ),
                        condition
                      );
                    }
                  }
                )
              )
              .toList()
          );

          if (JobConfigurationProperty.isOn(ctx.settings(), "drop-table")) {
            job.scheduleParts("cleanup", List.of(new DropTablePart("Destroy temp table (staging)", tempTableStaging)));
            job.scheduleParts("cleanup", List.of(new DropTablePart("Destroy temp table (final)", tempTableFinal)));
          }

          return job;
        }
      )
    );
  }
}
