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
import org.folio.anonymization.util.RandomValueUtils;
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
  private static final List<String> USER_AGENT_VALUES = RandomValueUtils.userAgents(USER_AGENT_POOL_SIZE);
  private static final String REPLACEMENT_SQL = RandomValueUtils.randomArrayEntryToJsonbSql(
    USER_AGENT_VALUES.toArray(new String[0])
  );

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
}
