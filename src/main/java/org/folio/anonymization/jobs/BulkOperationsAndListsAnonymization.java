package org.folio.anonymization.jobs;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.inline;

import java.util.List;
import org.folio.anonymization.config.JobConfig;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.db.TableReference;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.BatchGenerationFromTablePart;
import org.folio.anonymization.jobs.templates.DeletePart;
import org.folio.anonymization.jobs.templates.RedactPart;
import org.folio.anonymization.jobs.templates.ReplaceValuePart;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class BulkOperationsAndListsAnonymization implements JobFactory {

  private static final String REDACTED_FQL_QUERY = "{\"$and\":[]}";

  private static final String REPLACE_FQL_QUERY = "replace-fql-query";
  private static final String REPLACE_LIST_DETAILS_FQL_QUERY = "replace-list-details-fql-query";
  private static final String REPLACE_LIST_VERSIONS_FQL_QUERY = "replace-list-versions-fql-query";
  private static final String REPLACE_EXEC_CONTENT_IDENTIFIER = "replace-execution-content-identifier";
  private static final String REPLACE_RULE_DETAILS_INITIAL = "replace-rule-details-initial-value";
  private static final String REPLACE_RULE_DETAILS_UPDATED = "replace-rule-details-updated-value";
  private static final String CLEAR_PROFILE_RULE_COLLECTION = "clear-profile-rule-collection-for-users";

  private static final FieldReference BULK_OPERATION_ID = new FieldReference("bulk_operations", "bulk_operation", "id");
  private static final FieldReference BULK_OPERATION_EXEC_CONTENT_ID = new FieldReference(
    "bulk_operations",
    "bulk_operation_execution_content",
    "id"
  );
  private static final FieldReference BULK_OPERATION_RULE_DETAILS_ID = new FieldReference(
    "bulk_operations",
    "bulk_operation_rule_details",
    "id"
  );
  private static final FieldReference LIST_DETAILS_ID = new FieldReference("lists", "list_details", "id");
  private static final FieldReference LIST_VERSIONS_ID = new FieldReference("lists", "list_versions", "id");
  private static final FieldReference PROFILE_ID = new FieldReference("bulk_operations", "profile", "id");

  private static final FieldReference BULK_OPERATION_FQL_QUERY = new FieldReference(
    "bulk_operations",
    "bulk_operation",
    "fql_query"
  );
  private static final FieldReference LIST_DETAILS_FQL_QUERY = new FieldReference("lists", "list_details", "fql_query");
  private static final FieldReference LIST_VERSIONS_FQL_QUERY = new FieldReference("lists", "list_versions", "fql_query");
  private static final FieldReference BULK_OPERATION_EXEC_CONTENT_IDENTIFIER = new FieldReference(
    "bulk_operations",
    "bulk_operation_execution_content",
    "identifier"
  );
  private static final FieldReference BULK_OPERATION_RULE_DETAILS_INITIAL_VALUE = new FieldReference(
    "bulk_operations",
    "bulk_operation_rule_details",
    "initial_value"
  );
  private static final FieldReference BULK_OPERATION_RULE_DETAILS_UPDATED_VALUE = new FieldReference(
    "bulk_operations",
    "bulk_operation_rule_details",
    "updated_value"
  );
  private static final TableReference PROFILE_TABLE = new TableReference("bulk_operations", "profile");
  private static final FieldReference PROFILE_ENTITY_TYPE = new FieldReference("bulk_operations", "profile", "entity_type");

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    boolean hasBulkOperationTable = hasTable(tenant, "bulk_operations", "bulk_operation");
    boolean hasBulkOperationExecutionContentTable = hasTable(tenant, "bulk_operations", "bulk_operation_execution_content");
    boolean hasBulkOperationRuleDetailsTable = hasTable(tenant, "bulk_operations", "bulk_operation_rule_details");
    boolean hasProfileTable = hasTable(tenant, "bulk_operations", "profile");
    boolean hasListDetailsTable = hasTable(tenant, "lists", "list_details");
    boolean hasListVersionsTable = hasTable(tenant, "lists", "list_versions");

    List<JobConfigurationProperty> configuration = List.of(
      new JobConfigurationProperty(
        REPLACE_FQL_QUERY,
        "Replace mod_bulk_operations.bulk_operation.fql_query with redacted constant",
        true,
        !hasBulkOperationTable
      ),
      new JobConfigurationProperty(
        REPLACE_LIST_DETAILS_FQL_QUERY,
        "Replace mod_lists.list_details.fql_query with redacted constant",
        true,
        !hasListDetailsTable
      ),
      new JobConfigurationProperty(
        REPLACE_LIST_VERSIONS_FQL_QUERY,
        "Replace mod_lists.list_versions.fql_query with redacted constant",
        true,
        !hasListVersionsTable
      ),
      new JobConfigurationProperty(
        REPLACE_EXEC_CONTENT_IDENTIFIER,
        "Replace mod_bulk_operations.bulk_operation_execution_content.identifier with redacted constant",
        true,
        !hasBulkOperationExecutionContentTable
      ),
      new JobConfigurationProperty(
        REPLACE_RULE_DETAILS_INITIAL,
        "Replace mod_bulk_operations.bulk_operation_rule_details.initial_value with redacted constant",
        true,
        !hasBulkOperationRuleDetailsTable
      ),
      new JobConfigurationProperty(
        REPLACE_RULE_DETAILS_UPDATED,
        "Replace mod_bulk_operations.bulk_operation_rule_details.updated_value with redacted constant",
        true,
        !hasBulkOperationRuleDetailsTable
      ),
      new JobConfigurationProperty(
        CLEAR_PROFILE_RULE_COLLECTION,
        "Delete mod_bulk_operations.profile rows for USER entity_type",
        true,
        !hasProfileTable
      )
    );

    return List.of(
      new JobBuilder(
        "Bulk operations and lists anonymization",
        "Redacts bulk operation/list query fields and identifiers, and deletes USER profile rows.",
        tenant,
        context,
        configuration,
        ctx -> {
          Job job = new Job(ctx, List.of("prepare", "overwrite"));

          if (JobConfigurationProperty.isOn(ctx.settings(), REPLACE_FQL_QUERY)) {
            job.scheduleParts(
              "prepare",
              List.of(
                new BatchGenerationFromTablePart<>(
                  "Prepare to replace bulk_operations.bulk_operation.fql_query",
                  BULK_OPERATION_ID,
                  Object.class,
                  JobConfig.BATCH_SIZE,
                  "overwrite",
                  (label, condition, start, end) ->
                    new ReplaceValuePart(
                      "Replace bulk_operations.bulk_operation.fql_query on " + label,
                      BULK_OPERATION_FQL_QUERY,
                      condition,
                      field("cast({0} as text)", inline(REDACTED_FQL_QUERY))
                    )
                )
              )
            );
          }

          if (JobConfigurationProperty.isOn(ctx.settings(), REPLACE_LIST_DETAILS_FQL_QUERY)) {
            job.scheduleParts(
              "prepare",
              List.of(
                new BatchGenerationFromTablePart<>(
                  "Prepare to replace lists.list_details.fql_query",
                  LIST_DETAILS_ID,
                  Object.class,
                  JobConfig.BATCH_SIZE,
                  "overwrite",
                  (label, condition, start, end) ->
                    new ReplaceValuePart(
                      "Replace lists.list_details.fql_query on " + label,
                      LIST_DETAILS_FQL_QUERY,
                      condition,
                      field("cast({0} as text)", inline(REDACTED_FQL_QUERY))
                    )
                )
              )
            );
          }

          if (JobConfigurationProperty.isOn(ctx.settings(), REPLACE_LIST_VERSIONS_FQL_QUERY)) {
            job.scheduleParts(
              "prepare",
              List.of(
                new BatchGenerationFromTablePart<>(
                  "Prepare to replace lists.list_versions.fql_query",
                  LIST_VERSIONS_ID,
                  Object.class,
                  JobConfig.BATCH_SIZE,
                  "overwrite",
                  (label, condition, start, end) ->
                    new ReplaceValuePart(
                      "Replace lists.list_versions.fql_query on " + label,
                      LIST_VERSIONS_FQL_QUERY,
                      condition,
                      field("cast({0} as text)", inline(REDACTED_FQL_QUERY))
                    )
                )
              )
            );
          }

          if (JobConfigurationProperty.isOn(ctx.settings(), REPLACE_EXEC_CONTENT_IDENTIFIER)) {
            job.scheduleParts(
              "prepare",
              List.of(
                new BatchGenerationFromTablePart<>(
                  "Prepare to redact bulk_operations.bulk_operation_execution_content.identifier",
                  BULK_OPERATION_EXEC_CONTENT_ID,
                  Object.class,
                  JobConfig.BATCH_SIZE,
                  "overwrite",
                  (label, condition, start, end) ->
                    new RedactPart(
                      "Redact bulk_operations.bulk_operation_execution_content.identifier on " + label,
                      BULK_OPERATION_EXEC_CONTENT_IDENTIFIER,
                      condition
                    )
                )
              )
            );
          }

          if (JobConfigurationProperty.isOn(ctx.settings(), REPLACE_RULE_DETAILS_INITIAL)) {
            job.scheduleParts(
              "prepare",
              List.of(
                new BatchGenerationFromTablePart<>(
                  "Prepare to replace bulk_operations.bulk_operation_rule_details.initial_value",
                  BULK_OPERATION_RULE_DETAILS_ID,
                  Object.class,
                  JobConfig.BATCH_SIZE,
                  "overwrite",
                  (label, condition, start, end) ->
                    new RedactPart(
                      "Replace bulk_operations.bulk_operation_rule_details.initial_value on " + label,
                      BULK_OPERATION_RULE_DETAILS_INITIAL_VALUE,
                      condition
                    )
                )
              )
            );
          }

          if (JobConfigurationProperty.isOn(ctx.settings(), REPLACE_RULE_DETAILS_UPDATED)) {
            job.scheduleParts(
              "prepare",
              List.of(
                new BatchGenerationFromTablePart<>(
                  "Prepare to replace bulk_operations.bulk_operation_rule_details.updated_value",
                  BULK_OPERATION_RULE_DETAILS_ID,
                  Object.class,
                  JobConfig.BATCH_SIZE,
                  "overwrite",
                  (label, condition, start, end) ->
                    new RedactPart(
                      "Replace bulk_operations.bulk_operation_rule_details.updated_value on " + label,
                      BULK_OPERATION_RULE_DETAILS_UPDATED_VALUE,
                      condition
                    )
                )
              )
            );
          }

          if (JobConfigurationProperty.isOn(ctx.settings(), CLEAR_PROFILE_RULE_COLLECTION)) {
            job.scheduleParts(
              "prepare",
              List.of(
                new BatchGenerationFromTablePart<>(
                  "Prepare to delete bulk_operations.profile rows where entity_type=USER",
                  PROFILE_ID,
                  Object.class,
                  JobConfig.BATCH_SIZE,
                  "overwrite",
                  (label, condition, start, end) ->
                    new DeletePart(
                      "Delete bulk_operations.profile rows where entity_type=USER on " + label,
                      PROFILE_TABLE,
                      condition.and(PROFILE_ENTITY_TYPE.baseColumn(ctx.tenant().tenant(), String.class).eq("USER"))
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
