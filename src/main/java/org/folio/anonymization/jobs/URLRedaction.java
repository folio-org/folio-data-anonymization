package org.folio.anonymization.jobs;

import java.util.List;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.RedactPart;
import org.jooq.Condition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class URLRedaction implements JobFactory {

  private static final List<FieldReference> FIELDS = List.of(
    new FieldReference("agreements", "document_attachment", "da_url"),
    new FieldReference("agreements", "package_description_url", "pdu_url"),
    new FieldReference("agreements", "string_template", "strt_rule"),
    new FieldReference("copycat", "profile", "jsonb", "$.url"),
    new FieldReference("erm_usage", "usage_data_providers", "jsonb", "$.harvestingConfig.sushiConfig.serviceUrl"),
    new FieldReference("kb_ebsco_java", "kb_credentials", "url"),
    new FieldReference("licenses", "document_attachment", "da_url"),
    new FieldReference("organizations_storage", "contacts", "jsonb", "$.urls[*].value"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.contacts.urls[*].value"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.privilegedContacts.urls[*].value"),
    new FieldReference("organizations_storage", "interfaces", "jsonb", "$.uri"),
    new FieldReference("organizations_storage", "interfaces", "jsonb", "$.locallyStored"),
    new FieldReference("organizations_storage", "interfaces", "jsonb", "$.onlineLocation"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.urls[*].value"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.agreements[*].referenceUrl"),
    new FieldReference("organizations_storage", "privileged_contacts", "jsonb", "$.urls[*].value")
  );

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "URL redaction",
        "Redacts external service and organization contact URL values by replacing all alphanumeric characters",
        tenant,
        context,
        JobConfigurationProperty.fromFieldList(FIELDS, tenant),
        ctx ->
          new Job(ctx, List.of("redact"))
            .scheduleParts(
              "redact",
              JobConfigurationProperty
                .getEnabledFields(ctx.settings())
                .map(field -> {
                  Condition condition = field.baseColumn(ctx.tenant().tenant()).isNotNull();
                  return new RedactPart("Redact " + field.toString(), field, condition);
                })
                .toList()
            )
      )
    );
  }
}
