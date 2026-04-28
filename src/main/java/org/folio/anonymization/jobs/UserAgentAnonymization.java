package org.folio.anonymization.jobs;

import static org.jooq.impl.DSL.field;

import com.github.javafaker.Faker;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
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
public class UserAgentAnonymization implements JobFactory {

  private static final FieldReference USER_AGENT_FIELD = new FieldReference(
    "login",
    "event_logs",
    "jsonb",
    "$.userAgent"
  );

  private static final List<FieldReference> TARGET_FIELDS = List.of(USER_AGENT_FIELD);
  private static final int USER_AGENT_POOL_SIZE = 15;
  private static final List<String> USER_AGENT_VALUES = generateUserAgentValues(USER_AGENT_POOL_SIZE);
  private static final String REPLACEMENT_SQL = randomJsonbValueFromSetSql(USER_AGENT_VALUES);

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "User agent anonymization",
        "Replaces user agent values with randomized realistic-appearing values",
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
                  new ReplaceJSONBValuePart(
                    "replace user agent in " + field.toString(),
                    field,
                    field(REPLACEMENT_SQL, JSONB.class)
                  )
                )
                .toList()
            )
      )
    );
  }

  private static List<String> generateUserAgentValues(int size) {
    Faker faker = new Faker();
    ArrayList<String> values = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      values.add(faker.internet().userAgentAny());
    }
    return List.copyOf(values);
  }

  private static String randomJsonbValueFromSetSql(List<String> values) {
    String sqlValues = values
      .stream()
      .map(value -> "'" + value.replace("'", "''") + "'")
      .collect(Collectors.joining(", "));
    return "to_jsonb((ARRAY[%s])[1 + floor(random() * %d)::int])".formatted(sqlValues, values.size());
  }
}
