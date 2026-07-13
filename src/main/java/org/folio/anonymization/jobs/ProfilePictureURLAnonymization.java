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
public class ProfilePictureURLAnonymization implements JobFactory {

  private static final FieldReference FIELD = new FieldReference(
    "users",
    "users",
    "jsonb",
    "$.personal.profilePictureLink"
  );

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "profile_picture_links",
        "Profile picture URL anonymization",
        "Replaces personal profile picture URLs with a link to a random image",
        tenant,
        context,
        JobConfigurationProperty.fromFieldList(List.of(FIELD), tenant),
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
                        condition,
                        field(
                          """
                          to_jsonb(concat(
                            'https://placecats.com/',
                            (trunc(random() * 20) + 200),
                            '/',
                            (trunc(random() * 20) + 200)
                          ))
                          """,
                          JSONB.class
                        )
                      )
                  )
                )
                .toList()
            )
      )
    );
  }
}
