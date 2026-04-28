package org.folio.anonymization.jobs;

import static org.jooq.impl.DSL.field;

import java.util.List;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.ReplaceJSONBValuePart;
import org.jooq.JSONB;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DateOfBirthAnonymization implements JobFactory {

  private static final List<FieldReference> DATE_OF_BIRTH_FIELDS = List.of(
    new FieldReference("users", "users", "jsonb", "$.personal.dateOfBirth")
  );

  private static final String RANDOM_DOB_SQL =
    "to_jsonb(to_char(date '1940-01-01' + trunc(random() * ((date '2007-12-31' - date '1940-01-01') + 1))::int, 'YYYY-MM-DD'))";

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "Date of birth anonymization",
        "Replaces date of birth values with randomized realistic dates",
        tenant,
        context,
        JobConfigurationProperty.fromFieldList(DATE_OF_BIRTH_FIELDS, tenant),
        ctx ->
          new Job(ctx, List.of("overwrite"))
            .scheduleParts(
              "overwrite",
              JobConfigurationProperty
                .getEnabledFields(ctx.settings())
                .map(field ->
                  new ReplaceJSONBValuePart(
                    "replace date of birth in " + field.toString(),
                    field,
                    field(RANDOM_DOB_SQL, JSONB.class)
                  )
                )
                .toList()
            )
      )
    );
  }
}
