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
import org.folio.anonymization.jobs.templates.RedactPart;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ExportAndInvoiceCredentialRedaction implements JobFactory {

  private static final List<FieldReference> FIELDS = List.of(
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
    new FieldReference("invoice_storage", "batch_voucher_export_configs", "jsonb", "$.uploadURI"),
    new FieldReference("invoice_storage", "batch_voucher_export_configs", "jsonb", "$.ftpFormat"),
    new FieldReference("invoice_storage", "batch_voucher_export_configs", "jsonb", "$.ftpPort"),
    new FieldReference("invoice_storage", "export_config_credentials", "jsonb", "$.username"),
    new FieldReference("invoice_storage", "export_config_credentials", "jsonb", "$.password")
  );

  private static final FieldReference EXPORT_CONFIG_ID = new FieldReference("data_export_spring", "export_config", "id");
  private static final FieldReference JOB_ID = new FieldReference("data_export_spring", "job", "id");
  private static final FieldReference BATCH_VOUCHER_EXPORT_CONFIGS_ID = new FieldReference(
    "invoice_storage",
    "batch_voucher_export_configs",
    "id"
  );
  private static final FieldReference EXPORT_CONFIG_CREDENTIALS_ID = new FieldReference(
    "invoice_storage",
    "export_config_credentials",
    "id"
  );

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "Export and invoice credential redaction",
        "Redacts FTP endpoint and credential fields in data export spring and invoice storage tables.",
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
                    batchIdField(targetField),
                    Object.class,
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

  private static FieldReference batchIdField(FieldReference field) {
    if ("data_export_spring".equals(field.schema())) {
      if ("export_config".equals(field.table())) {
        return EXPORT_CONFIG_ID;
      }
      if ("job".equals(field.table())) {
        return JOB_ID;
      }
    }

    if ("invoice_storage".equals(field.schema())) {
      if ("batch_voucher_export_configs".equals(field.table())) {
        return BATCH_VOUCHER_EXPORT_CONFIGS_ID;
      }
      if ("export_config_credentials".equals(field.table())) {
        return EXPORT_CONFIG_CREDENTIALS_ID;
      }
    }

    throw new IllegalArgumentException("No batch ID field configured for " + field);
  }
}
