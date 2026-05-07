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

  @Autowired
  private SharedExecutionContext context;

  @SuppressWarnings("unchecked")
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
