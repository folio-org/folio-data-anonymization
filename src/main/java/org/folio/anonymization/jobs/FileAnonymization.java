package org.folio.anonymization.jobs;

import static org.jooq.impl.DSL.field;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Stream;
import org.folio.anonymization.config.JobConfig;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.JobPart;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.BatchGenerationFromTablePart;
import org.folio.anonymization.jobs.templates.ReplaceOIDPart;
import org.folio.anonymization.jobs.templates.ReplaceValueFromListPart;
import org.folio.anonymization.service.SeedFileService;
import org.folio.anonymization.util.RandomValueUtils;
import org.jooq.Condition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class FileAnonymization implements JobFactory {

  private static final FieldReference BATCH_PRINT_HEX_FIELD = new FieldReference("batch_print", "printing", "content");
  private static final FieldReference ERM_USAGE_BYTES_FIELD = new FieldReference("erm_usage", "files", "data");
  private static final FieldReference INVOICE_STORAGE_B64_FIELD = new FieldReference(
    "invoice_storage",
    "documents",
    "document_data"
  );
  private static final List<FieldReference> POSTGRES_LO_FIELDS = List.of(
    new FieldReference("agreements", "file_object", "file_contents"),
    new FieldReference("agreements", "comparison_job", "cj_file_contents"),
    new FieldReference("licenses", "file_object", "file_contents")
  );

  private final SharedExecutionContext context;

  private final List<byte[]> seedFiles;

  @Autowired
  public FileAnonymization(SharedExecutionContext context, SeedFileService seedFileService) {
    this.context = context;
    this.seedFiles = seedFileService.getSeedFilesAsBytes("pdf-files/*.pdf");
  }

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "File anonymization",
        "Replaces stored file blobs with placeholder safe values.",
        tenant,
        context,
        JobConfigurationProperty.fromFieldList(
          Stream
            .concat(
              Stream.of(BATCH_PRINT_HEX_FIELD, ERM_USAGE_BYTES_FIELD, INVOICE_STORAGE_B64_FIELD),
              POSTGRES_LO_FIELDS.stream()
            )
            .toList(),
          tenant
        ),
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
                      getPart("replace %s on %s".formatted(field.toString(), label), field, condition)
                  )
                )
                .toList()
            )
      )
    );
  }

  private JobPart getPart(String label, FieldReference field, Condition condition) {
    if (field.equals(BATCH_PRINT_HEX_FIELD)) {
      return new ReplaceValueFromListPart(
        label,
        field,
        condition,
        seedFiles.stream().map(RandomValueUtils::encodeHexUppercase).toList()
      );
    } else if (field.equals(ERM_USAGE_BYTES_FIELD)) {
      return new ReplaceValueFromListPart(
        label,
        List.of(field),
        condition,
        List.of(seedFiles),
        List.of(field("replacement", byte[].class))
      );
    } else if (field.equals(INVOICE_STORAGE_B64_FIELD)) {
      return new ReplaceValueFromListPart(
        label,
        field,
        condition,
        seedFiles.stream().map(RandomValueUtils::encodeBase64).map(s -> "data:application/pdf;base64," + s).toList()
      );
    } else if (POSTGRES_LO_FIELDS.contains(field)) {
      return new ReplaceOIDPart(
        label,
        field,
        condition,
        "Replaced during anonymization".getBytes(StandardCharsets.UTF_8)
      );
    } else {
      throw new IllegalArgumentException("No replacements defined for field: " + field);
    }
  }
}
