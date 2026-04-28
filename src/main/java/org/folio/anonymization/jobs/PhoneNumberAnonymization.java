package org.folio.anonymization.jobs;

import java.util.List;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.ReplaceJSONBWithSQLPart;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PhoneNumberAnonymization implements JobFactory {

  private static final List<FieldReference> PHONE_NUMBER_FIELDS = List.of(
    new FieldReference("inn_reach", "transaction_local_hold", "patron_phone"),
    new FieldReference("oa", "party", "p_phone"),
    new FieldReference("oa", "party", "p_mobile"),
    new FieldReference("organizations_storage", "contacts", "jsonb", "$.phoneNumbers[*].phoneNumber"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.contacts.phoneNumbers[*].phoneNumber"),
    new FieldReference(
      "organizations_storage",
      "organizations",
      "jsonb",
      "$.privilegedContacts.phoneNumbers[*].phoneNumber"
    ),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.phoneNumbers[*].phoneNumber"),
    new FieldReference("organizations_storage", "privileged_contacts", "jsonb", "$.phoneNumbers[*].phoneNumber"),
    new FieldReference("users", "staging_users", "jsonb", "$.contactInfo.phone"),
    new FieldReference("users", "staging_users", "jsonb", "$.contactInfo.mobilePhone"),
    new FieldReference("users", "user_tenant", "phone_number"),
    new FieldReference("users", "user_tenant", "mobile_phone_number"),
    new FieldReference("users", "users", "jsonb", "$.personal.phone"),
    new FieldReference("users", "users", "jsonb", "$.personal.mobilePhone")
  );

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "Phone number anonymization",
        "Replaces user's phone numbers with randomized values",
        tenant,
        context,
        JobConfigurationProperty.fromFieldList(PHONE_NUMBER_FIELDS, tenant),
        ctx ->
          new Job(ctx, List.of("overwrite"))
            .scheduleParts(
              "overwrite",
              JobConfigurationProperty
                .getEnabledFields(ctx.settings())
                .map(field ->
                  new ReplaceJSONBWithSQLPart(
                    "replace phone number in " + field.toString(),
                    field,
                    // 978 = Ipswich, MA
                    // 919 = Durham, NC
                    // 512 = Austin, TX
                    """
                      concat(
                        '\"(',
                        ('{978,919,512}'::text[])[floor(random() * 3 + 1)],
                        ') 555-',
                        trunc(random() * 10),
                        trunc(random() * 10),
                        trunc(random() * 10),
                        trunc(random() * 10),
                        '\"'
                      )::jsonb
                    """
                  )
                )
                .toList()
            )
      )
    );
  }
}
