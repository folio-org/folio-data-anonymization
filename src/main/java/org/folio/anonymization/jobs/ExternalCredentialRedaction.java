package org.folio.anonymization.jobs;

import java.util.List;
import org.folio.anonymization.config.JobConfig;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.db.TableIDs;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.BatchGenerationFromTablePart;
import org.folio.anonymization.jobs.templates.RedactPart;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ExternalCredentialRedaction implements JobFactory {

  private static final List<FieldReference> FIELDS = List.of(
    new FieldReference("copycat", "profile", "jsonb", "$.authentication"),
    new FieldReference(
      "data_export_spring",
      "export_config",
      "export_type_specific_parameters",
      "$.vendorEdiOrdersExportConfig.ediFtp.serverAddress"
    ),
    new FieldReference(
      "data_export_spring",
      "export_config",
      "export_type_specific_parameters",
      "$.vendorEdiOrdersExportConfig.ediFtp.username"
    ),
    new FieldReference(
      "data_export_spring",
      "export_config",
      "export_type_specific_parameters",
      "$.vendorEdiOrdersExportConfig.ediFtp.password"
    ),
    new FieldReference(
      "data_export_spring",
      "job",
      "export_type_specific_parameters",
      "$.vendorEdiOrdersExportConfig.ediFtp.serverAddress"
    ),
    new FieldReference(
      "data_export_spring",
      "job",
      "export_type_specific_parameters",
      "$.vendorEdiOrdersExportConfig.ediFtp.username"
    ),
    new FieldReference(
      "data_export_spring",
      "job",
      "export_type_specific_parameters",
      "$.vendorEdiOrdersExportConfig.ediFtp.password"
    ),
    new FieldReference("email", "settings", "jsonb", "$.host"),
    new FieldReference("email", "settings", "jsonb", "$.username"),
    new FieldReference("email", "settings", "jsonb", "$.password"),
    new FieldReference("email", "settings", "jsonb", "$.from"),
    new FieldReference("email", "settings", "jsonb", "$.value.host"),
    new FieldReference("email", "settings", "jsonb", "$.value.username"),
    new FieldReference("email", "settings", "jsonb", "$.value.password"),
    new FieldReference("email", "settings", "jsonb", "$.value.from"),
    new FieldReference("email", "smtp_configuration", "jsonb", "$.host"),
    new FieldReference("email", "smtp_configuration", "jsonb", "$.username"),
    new FieldReference("email", "smtp_configuration", "jsonb", "$.password"),
    new FieldReference("email", "smtp_configuration", "jsonb", "$.from"),
    new FieldReference("email", "smtp_configuration", "jsonb", "$.value.host"),
    new FieldReference("email", "smtp_configuration", "jsonb", "$.value.username"),
    new FieldReference("email", "smtp_configuration", "jsonb", "$.value.password"),
    new FieldReference("email", "smtp_configuration", "jsonb", "$.value.from"),
    new FieldReference("erm_usage", "usage_data_providers", "jsonb", "$.sushiCredentials.apiKey"),
    new FieldReference("inn_reach", "central_server_credentials", "central_server_key"),
    new FieldReference("inn_reach", "central_server_credentials", "central_server_secret"),
    new FieldReference("inn_reach", "local_server_credentials", "local_server_key"),
    new FieldReference("inn_reach", "local_server_credentials", "local_server_secret"),
    new FieldReference("invoice_storage", "batch_voucher_export_configs", "jsonb", "$.uploadURI"),
    new FieldReference("invoice_storage", "batch_voucher_export_configs", "jsonb", "$.ftpFormat"),
    new FieldReference("invoice_storage", "batch_voucher_export_configs", "jsonb", "$.ftpPort"),
    new FieldReference("invoice_storage", "export_config_credentials", "jsonb", "$.username"),
    new FieldReference("invoice_storage", "export_config_credentials", "jsonb", "$.password"),
    new FieldReference("kb_ebsco_java", "kb_credentials", "api_key"),
    new FieldReference("organizations_storage", "interfaces", "jsonb", "$.username"),
    new FieldReference("organizations_storage", "interfaces", "jsonb", "$.password"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.edi.ediFtp.serverAddress"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.edi.ediFtp.username"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.edi.ediFtp.password"),
    new FieldReference("remote_storage", "remote_storage_configurations", "api_key")
  );

  @Autowired
  private SharedExecutionContext context;

  @SuppressWarnings("unchecked")
  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "external_credentials",
        "External credential redaction",
        "Redacts credentials for external services (FTP, INN-Reach, SMTP, Z39.50, etc.)",
        tenant,
        context,
        JobConfigurationProperty.fromFieldList(FIELDS, tenant),
        ctx ->
          new Job(ctx, List.of("prepare", "redact"))
            .scheduleParts(
              "prepare",
              JobConfigurationProperty
                .getEnabledFields(ctx.settings())
                .map(targetField ->
                  new BatchGenerationFromTablePart<>(
                    "Prepare to redact " + targetField.toString(),
                    TableIDs.getIdFor(targetField).getLeft(),
                    (Class<Object>) TableIDs.getIdFor(targetField).getRight(),
                    JobConfig.BATCH_SIZE,
                    "redact",
                    (label, condition, start, end) ->
                      new RedactPart("Redact " + targetField.toString() + " on " + label, targetField, condition)
                  )
                )
                .toList()
            )
      )
    );
  }
}
