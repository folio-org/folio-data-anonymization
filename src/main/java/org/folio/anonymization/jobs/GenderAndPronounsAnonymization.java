package org.folio.anonymization.jobs;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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
public class GenderAndPronounsAnonymization implements JobFactory {

  private static final FieldReference USER_GENDER_FIELD = new FieldReference(
    "users",
    "users",
    "jsonb",
    "$.personal.gender"
  );
  private static final FieldReference USER_PRONOUNS_FIELD = new FieldReference(
    "users",
    "users",
    "jsonb",
    "$.personal.pronouns"
  );

  private static final List<FieldReference> TARGET_FIELDS = List.of(USER_GENDER_FIELD, USER_PRONOUNS_FIELD);

  private static final Map<String, String> REPLACEMENT_SQL_BY_JSON_PATH = Map.of(
    USER_GENDER_FIELD.jsonPath(),
    randomJsonbValueFromSetSql(List.of("Female", "Male", "Non-binary", "Prefer not to say")),
    USER_PRONOUNS_FIELD.jsonPath(),
    randomJsonbValueFromSetSql(List.of("she/her", "he/him", "they/them", "ze/zir"))
  );

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "Gender/pronouns anonymization",
        "Replaces personal gender and pronouns with randomized realistic-appearing values",
        tenant,
        context,
        JobConfigurationProperty.fromFieldList(TARGET_FIELDS, tenant),
        ctx ->
          new Job(ctx, List.of("overwrite"))
            .scheduleParts(
              "overwrite",
              JobConfigurationProperty
                .getEnabledFields(ctx.settings())
                .map(field ->
                  new ReplaceJSONBWithSQLPart(
                    "replace " + field.jsonPath(),
                          field,
                          REPLACEMENT_SQL_BY_JSON_PATH.get(field.jsonPath())
                  )
                )
                 .toList()
            )
      )
    );
  }

  private static String randomJsonbValueFromSetSql(List<String> values) {
    String sqlValues = values
      .stream()
      .map(value -> "'" + value.replace("'", "''") + "'")
      .collect(Collectors.joining(", "));
    return "to_jsonb((ARRAY[%s])[1 + floor(random() * %d)::int])".formatted(sqlValues, values.size());
  }
}
