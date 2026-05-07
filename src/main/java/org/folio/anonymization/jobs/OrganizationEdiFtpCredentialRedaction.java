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
public class OrganizationEdiFtpCredentialRedaction implements JobFactory {

  private static final List<FieldReference> FIELDS = List.of(
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.edi.ediFtp.serverAddress"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.edi.ediFtp.username"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.edi.ediFtp.password")
  );

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "Organization EDI FTP credential redaction",
        "Redacts organizations.edi.ediFtp server address, username, and password values.",
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
                    targetField,
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
