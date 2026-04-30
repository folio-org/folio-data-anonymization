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
public class FilenameRedaction implements JobFactory {

  private static final List<FieldReference> FIELDS = List.of(
    new FieldReference("agreements", "file_upload", "fu_filename"),
    new FieldReference("data_export", "file_definitions", "jsonb", "$.filename"),
    new FieldReference("data_export", "file_definitions", "jsonb", "$.sourcePath"),
    new FieldReference(
      "data_export_spring",
      "export_config",
      "export_type_specific_parameters",
      "$.vendorEdiOrdersExportConfig.ediFtp.orderDirectory"
    ),
    new FieldReference(
      "data_export_spring",
      "export_config",
      "export_type_specific_parameters",
      "$.vendorEdiOrdersExportConfig.ediFtp.invoiceDirectory"
    ),
    new FieldReference(
      "data_export_spring",
      "job",
      "export_type_specific_parameters",
      "$.vendorEdiOrdersExportConfig.ediFtp.orderDirectory"
    ),
    new FieldReference(
      "data_export_spring",
      "job",
      "export_type_specific_parameters",
      "$.vendorEdiOrdersExportConfig.ediFtp.invoiceDirectory"
    ),
    new FieldReference("data_import", "upload_definitions", "jsonb", "$.fileDefinitions[*].name"),
    new FieldReference("data_import", "upload_definitions", "jsonb", "$.fileDefinitions[*].sourcePath"),
    new FieldReference("data_import", "upload_definitions", "jsonb", "$.fileDefinitions[*].uiKey"),
    new FieldReference("erm_usage", "custom_reports", "jsonb", "$.fileName"),
    new FieldReference("licenses", "file_upload", "fu_filename"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.edi.ediFtp.orderDirectory"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.edi.ediFtp.invoiceDirectory"),
    new FieldReference("source_record_manager", "job_execution", "source_path"),
    new FieldReference("source_record_manager", "job_execution", "file_name")
  );

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "Filename and S3 path redaction",
        "Redacts filenames and S3 paths by replacing all alphanumeric characters with 'X' and '1'.",
        tenant,
        context,
        JobConfigurationProperty.fromFieldList(FIELDS, tenant),
        ctx ->
          new Job(ctx, List.of("prepare", "redact"))
            .scheduleParts(
              "prepare",
              JobConfigurationProperty
                .getEnabledFields(ctx.settings())
                .map(field ->
                  new BatchGenerationFromTablePart<>(
                    "Prepare to redact " + field.toString(),
                    field,
                    JobConfig.BATCH_SIZE,
                    "redact",
                    (label, condition, start, end) ->
                      new RedactPart("Redact " + field.toString() + " on " + label, field, condition)
                  )
                )
                .toList()
            )
      )
    );
  }
}
