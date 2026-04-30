package org.folio.anonymization.jobs;

import static org.jooq.impl.DSL.field;

import java.util.List;
import java.util.Map;
import org.folio.anonymization.config.JobConfig;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.BatchGenerationFromTablePart;
import org.folio.anonymization.jobs.templates.ReplaceJSONBValuePart;
import org.folio.anonymization.util.RandomValueUtils;
import org.jooq.JSONB;
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
    RandomValueUtils.randomArrayEntryToJsonbSql("Female", "Male", "Non-binary", "Prefer not to say"),
    USER_PRONOUNS_FIELD.jsonPath(),
    RandomValueUtils.randomArrayEntryToJsonbSql("she/her", "he/him", "they/them", "ze/zir")
  );

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "Gender/pronouns anonymization",
        "Replaces personal gender and pronouns with values from a predefined set",
        tenant,
        context,
        JobConfigurationProperty.fromFieldList(TARGET_FIELDS, tenant),
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
                      new ReplaceJSONBValuePart(
                        "replace %s on %s".formatted(field.toString(), label),
                        field,
                        i -> field(REPLACEMENT_SQL_BY_JSON_PATH.get(field.jsonPath()), JSONB.class),
                        condition
                      )
                  )
                )
                .toList()
            )
      )
    );
  }
}
