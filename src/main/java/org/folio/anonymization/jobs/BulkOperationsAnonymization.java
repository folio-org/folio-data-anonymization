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
import org.folio.anonymization.jobs.templates.ReplaceValuePart;
import org.jooq.JSONB;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class BulkOperationsAnonymization implements JobFactory {

  private static final String REDACTED = "redacted";
  private static final String REDACTED_FQL_QUERY = "{\"$and\":[]}";

  private static final String REPLACE_FQL_QUERY = "replace-fql-query";
  private static final String REPLACE_EXEC_CONTENT_IDENTIFIER = "replace-execution-content-identifier";
  private static final String REPLACE_RULE_DETAILS_INITIAL = "replace-rule-details-initial-value";
  private static final String REPLACE_RULE_DETAILS_UPDATED = "replace-rule-details-updated-value";
  private static final String CLEAR_PROFILE_RULE_COLLECTION = "clear-profile-rule-collection-for-users";

  private static final FieldReference BULK_OPERATION_FQL_QUERY = new FieldReference(
    "bulk_operations",
    "bulk_operation",
    "fql_query"
  );
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
  private static final FieldReference PROFILE_RULE_COLLECTION = new FieldReference(
    "bulk_operations",
    "profile",
    "bulk_operation_rule_collection"
  );
  private static final FieldReference PROFILE_ENTITY_TYPE = new FieldReference("bulk_operations", "profile", "entity_type");

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    boolean hasBulkOperationTable = hasTable(tenant, "bulk_operations", "bulk_operation");
    boolean hasBulkOperationExecutionContentTable = hasTable(tenant, "bulk_operations", "bulk_operation_execution_content");
    boolean hasBulkOperationRuleDetailsTable = hasTable(tenant, "bulk_operations", "bulk_operation_rule_details");
    boolean hasProfileTable = hasTable(tenant, "bulk_operations", "profile");

    List<JobConfigurationProperty> configuration = List.of(
      new JobConfigurationProperty(
        REPLACE_FQL_QUERY,
        "Replace mod_bulk_operations.bulk_operation.fql_query with redacted constant",
        true,
        !hasBulkOperationTable
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
        "Replace mod_bulk_operations.profile.bulk_operation_rule_collection with [] for USER entity_type",
        true,
        !hasProfileTable
      )
    );

    return List.of(
      new JobBuilder(
        "Bulk operations anonymization",
        "Redacts bulk operation free-text identifiers and clears user profile rule collections.",
        tenant,
        context,
        configuration,
        ctx -> {
          Job job = new Job(ctx, List.of("overwrite"));
          List<JobPart> parts = new ArrayList<>();

          if (JobConfigurationProperty.isOn(ctx.settings(), REPLACE_FQL_QUERY)) {
            parts.add(
              new ReplaceValuePart(
                "Replace bulk_operations.bulk_operation.fql_query",
                BULK_OPERATION_FQL_QUERY,
                trueCondition(),
                field("cast({0} as text)", inline(REDACTED_FQL_QUERY))
              )
            );
          }

          if (JobConfigurationProperty.isOn(ctx.settings(), REPLACE_EXEC_CONTENT_IDENTIFIER)) {
            parts.add(
              new ReplaceValuePart(
                "Replace bulk_operations.bulk_operation_execution_content.identifier",
                BULK_OPERATION_EXEC_CONTENT_IDENTIFIER,
                trueCondition(),
                field("cast({0} as text)", inline(REDACTED))
              )
            );
          }

          if (JobConfigurationProperty.isOn(ctx.settings(), REPLACE_RULE_DETAILS_INITIAL)) {
            parts.add(
              new ReplaceValuePart(
                "Replace bulk_operations.bulk_operation_rule_details.initial_value",
                BULK_OPERATION_RULE_DETAILS_INITIAL_VALUE,
                trueCondition(),
                field("cast({0} as text)", inline(REDACTED))
              )
            );
          }

          if (JobConfigurationProperty.isOn(ctx.settings(), REPLACE_RULE_DETAILS_UPDATED)) {
            parts.add(
              new ReplaceValuePart(
                "Replace bulk_operations.bulk_operation_rule_details.updated_value",
                BULK_OPERATION_RULE_DETAILS_UPDATED_VALUE,
                trueCondition(),
                field("cast({0} as text)", inline(REDACTED))
              )
            );
          }

          if (JobConfigurationProperty.isOn(ctx.settings(), CLEAR_PROFILE_RULE_COLLECTION)) {
            parts.add(
              new ReplaceValuePart(
                "Replace bulk_operations.profile.bulk_operation_rule_collection where entity_type=USER",
                PROFILE_RULE_COLLECTION,
                PROFILE_ENTITY_TYPE.baseColumn(ctx.tenant().tenant(), String.class).eq("USER"),
                field("'[]'::jsonb", JSONB.class)
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
