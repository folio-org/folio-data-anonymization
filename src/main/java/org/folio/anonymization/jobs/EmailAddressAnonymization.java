package org.folio.anonymization.jobs;

import java.util.List;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.BatchGenerationFromTablePart;
import org.folio.anonymization.jobs.templates.ReplaceValueFromListPart;
import org.folio.anonymization.util.RandomValueUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class EmailAddressAnonymization implements JobFactory {

  private static final int BATCH_SIZE = 2_000;

  private static final List<FieldReference> FIELDS = List.of(
    new FieldReference("batch_print", "printing", "sorting_field"),
    new FieldReference("erm_usage", "usage_data_providers", "jsonb", "$.sushiCredentials.requestorMail"),
    new FieldReference("oa", "alternate_email_address", "aea_email"),
    new FieldReference("oa", "party", "p_main_email"),
    new FieldReference("organizations_storage", "contacts", "jsonb", "$.emails[*].value"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.contacts.emails[*].value"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.privilegedContacts.emails[*].value"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.emails[*].value"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.edi.ediJob.sendToEmails"),
    new FieldReference("organizations_storage", "privileged_contacts", "jsonb", "$.emails[*].value"),
    new FieldReference("users", "staging_users", "jsonb", "$.contactInfo.email"),
    new FieldReference("users", "user_tenant", "email"),
    new FieldReference("users", "users", "jsonb", "$.personal.email"),
    // administratorEmail only exists on one row (where config_name = 'general'), however, this property
    // only exists on that specific row, so there's no harm in including the other rows in the scope
    // (they will simply be no-ops)
    new FieldReference("oai_pmh", "configuration_settings", "config_value", "$.administratorEmail")
  );

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "Email address anonymization",
        "Replaces email addresses with semi-unique anonymized values",
        tenant,
        context,
        JobConfigurationProperty.fromFieldList(FIELDS, tenant),
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
                    BATCH_SIZE,
                    "overwrite",
                    (label, condition, start, end) ->
                      new ReplaceValueFromListPart(
                        "replace %s on %s".formatted(field.toString(), label),
                        field,
                        condition,
                        RandomValueUtils.emails(Math.max(end - start, 5))
                      )
                  )
                )
                .toList()
            )
      )
    );
  }
}
