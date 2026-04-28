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
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.phoneNumbers[*].phoneNumber")
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
                    "replace phone number",
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
