package org.folio.anonymization.jobs;

import static org.jooq.impl.DSL.field;

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
import org.folio.anonymization.jobs.templates.ReplaceJSONBValuePart;
import org.jooq.JSONB;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class IPAddressAnonymization implements JobFactory {

  private static final List<FieldReference> IP_FIELDS = List.of(
    new FieldReference("login", "event_logs", "jsonb", "$.ip")
  );

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "IP address anonymization",
        "Replaces user's IP addresses with randomized values",
        tenant,
        context,
        JobConfigurationProperty.fromFieldList(IP_FIELDS, tenant),
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
                        "replace IPs in %s on %s".formatted(field.toString(), label),
                        field,
                        i ->
                          field(
                            "concat('\"169.254.', trunc(random() * 256), '.', trunc(random() * 256), '\"')::jsonb",
                            JSONB.class
                          ),
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
