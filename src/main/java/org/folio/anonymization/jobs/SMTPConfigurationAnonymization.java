package org.folio.anonymization.jobs;

import static org.jooq.impl.DSL.trueCondition;

import java.util.List;
import java.util.stream.Stream;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.RedactPart;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class SMTPConfigurationAnonymization implements JobFactory {

  private static final List<FieldReference> FIELDS = List.of(
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
    new FieldReference("email", "smtp_configuration", "jsonb", "$.value.from")
  );

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "SMTP configuration anonymization",
        "Replaces SMTP credentials in mod_email",
        tenant,
        context,
        JobConfigurationProperty.fromFieldList(FIELDS, tenant),
        ctx ->
          new Job(ctx, List.of("overwrite"))
            .scheduleParts(
              "overwrite",
              JobConfigurationProperty
                .getEnabledFields(ctx.settings())
                .flatMap(field ->
                  Stream.of(new RedactPart("Redact SMTP configuration in " + field, field, trueCondition()))
                )
                .toList()
            )
      )
    );
  }
}
