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
import org.folio.anonymization.jobs.templates.ReplaceValueFromListPart;
import org.folio.anonymization.util.RandomValueUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class NamePrefixAnonymization implements JobFactory {

  private static final List<FieldReference> NAME_PREFIX_FIELDS = List.of(
    new FieldReference("oa", "party", "p_title"),
    new FieldReference("organizations_storage", "contacts", "jsonb", "$.prefix"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.contacts[*].prefix"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.privilegedContacts[*].prefix"),
    new FieldReference("organizations_storage", "privileged_contacts", "jsonb", "$.prefix")
  );

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "Name prefix anonymization",
        "Replaces name-prefix values with Faker-generated realistic-appearing values",
        tenant,
        context,
        JobConfigurationProperty.fromFieldList(NAME_PREFIX_FIELDS, tenant),
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
                      new ReplaceValueFromListPart(
                        "replace %s on %s".formatted(field.toString(), label),
                        field,
                        condition,
                        RandomValueUtils.namePrefixes(Math.max(end - start, 5))
                      )
                  )
                )
                .toList()
            )
      )
    );
  }
}
