package org.folio.anonymization.jobs;

import java.util.List;
import org.folio.anonymization.domain.db.TableReference;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.TruncateTablePart;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TableTruncation implements JobFactory {

  private static final List<TableReference> LOGGING_AUDIT_TABLES_TO_TRUNCATE = List.of(
    new TableReference("agreements", "log_entry_additional_info"),
    new TableReference("agreements", "log_entry"),
    new TableReference("audit", "acquisition_invoice_line_log"),
    new TableReference("audit", "acquisition_invoice_log"),
    new TableReference("audit", "acquisition_order_line_log"),
    new TableReference("audit", "acquisition_order_log"),
    new TableReference("audit", "acquisition_organization_log"),
    new TableReference("audit", "acquisition_piece_log"),
    new TableReference("audit", "audit_data"),
    new TableReference("audit", "circulation_logs"),
    new TableReference("audit", "holdings_audit"),
    new TableReference("audit", "instance_audit"),
    new TableReference("audit", "item_audit"),
    new TableReference("audit", "marc_authority_audit"),
    new TableReference("audit", "marc_bib_audit"),
    new TableReference("audit", "user_audit"),
    new TableReference("configuration", "audit_config_data"),
    new TableReference("dcb", "transactions_audit"),
    new TableReference("email", "email_statistics"),
    new TableReference("notify", "notify_data"),
    new TableReference("pubsub", "audit_message_payload")
  );

  private static final List<TableReference> EPHEMERAL_TABLES_TO_TRUNCATE = List.of(
    new TableReference("data_export_worker", "batch_job_execution_context"),
    new TableReference("data_export_worker", "batch_step_execution_context"),
    new TableReference("data_export_worker", "batch_job_execution_params"),
    new TableReference("data_export_worker", "e_holdings_package"),
    new TableReference("data_export_worker", "e_holdings_resource"),
    new TableReference("fqm_manager", "query_details"),
    new TableReference("fqm_manager", "query_results"),
    new TableReference("inn_reach", "batch_job_execution_context"),
    new TableReference("inn_reach", "batch_job_execution_params"),
    new TableReference("inn_reach", "batch_step_execution_context"),
    new TableReference("lists", "list_contents"),
    new TableReference("orders_storage", "outbox_event_log"),
    new TableReference("organizations_storage", "outbox_event_log"),
    new TableReference("users", "outbox_event_log")
  );

  private static final List<TableReference> CREDENTIAL_TABLES_TO_TRUNCATE = List.of(
    new TableReference("authtoken", "api_tokens"),
    new TableReference("authtoken", "job"),
    new TableReference("authtoken", "refresh_tokens"),
    new TableReference("erm_usage", "aggregator_settings"),
    new TableReference("kb_ebsco_java", "usage_consolidation_credentials")
  );

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "Logging/audit table truncation",
        "Truncates logging and audit tables that cannot be safely anonymized",
        tenant,
        context,
        JobConfigurationProperty.fromTableList(LOGGING_AUDIT_TABLES_TO_TRUNCATE, tenant),
        ctx ->
          new Job(ctx, List.of("truncate"))
            .scheduleParts(
              "truncate",
              JobConfigurationProperty.getEnabledTables(ctx.settings()).map(TruncateTablePart::new).toList()
            )
      ),
      new JobBuilder(
        "Ephemeral table truncation",
        "Truncates temporary tables — such as outboxes, in-progress exports, or cached data — that cannot be safely anonymized",
        tenant,
        context,
        JobConfigurationProperty.fromTableList(EPHEMERAL_TABLES_TO_TRUNCATE, tenant),
        ctx ->
          new Job(ctx, List.of("truncate"))
            .scheduleParts(
              "truncate",
              JobConfigurationProperty.getEnabledTables(ctx.settings()).map(TruncateTablePart::new).toList()
            )
      ),
      new JobBuilder(
        "Credential table truncation",
        "Truncates tables containing credentials and access tokens that cannot be safely anonymized",
        tenant,
        context,
        JobConfigurationProperty.fromTableList(CREDENTIAL_TABLES_TO_TRUNCATE, tenant),
        ctx ->
          new Job(ctx, List.of("truncate"))
            .scheduleParts(
              "truncate",
              JobConfigurationProperty.getEnabledTables(ctx.settings()).map(TruncateTablePart::new).toList()
            )
      )
    );
  }
}
