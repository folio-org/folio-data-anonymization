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
public class ImportProfileRelationshipsAnonymization implements JobFactory {

  private static final List<FieldReference> FIELDS = List.of(
    new FieldReference("di_converter_storage", "action_profiles", "jsonb", "$.parentProfiles"),
    new FieldReference("di_converter_storage", "action_profiles", "jsonb", "$.childProfiles"),
    new FieldReference("di_converter_storage", "job_profiles", "jsonb", "$.parentProfiles"),
    new FieldReference("di_converter_storage", "job_profiles", "jsonb", "$.childProfiles"),
    new FieldReference("di_converter_storage", "mapping_profiles", "jsonb", "$.parentProfiles"),
    new FieldReference("di_converter_storage", "mapping_profiles", "jsonb", "$.childProfiles"),
    new FieldReference("di_converter_storage", "match_profiles", "jsonb", "$.parentProfiles"),
    new FieldReference("di_converter_storage", "match_profiles", "jsonb", "$.childProfiles"),
    new FieldReference("di_converter_storage", "profile_snapshots", "jsonb", "$.parentProfiles"),
    new FieldReference("di_converter_storage", "profile_snapshots", "jsonb", "$.childProfiles"),
    new FieldReference("di_converter_storage", "profile_snapshots", "jsonb", "$.childSnapshotWrappers")
  );

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "Import profile relationship anonymization",
        "Replaces parent/child profile relationship arrays with empty arrays in data import converter tables.",
        tenant,
        context,
        JobConfigurationProperty.fromFieldList(FIELDS, tenant),
        ctx ->
          new Job(ctx, List.of("prepare", "overwrite"))
            .scheduleParts(
              "prepare",
              JobConfigurationProperty
                .getEnabledFields(ctx.settings())
                .map(targetField ->
                  new BatchGenerationFromTablePart<>(
                    "Prepare to replace " + targetField.toString(),
                    targetField,
                    JobConfig.BATCH_SIZE,
                    "overwrite",
                    (label, condition, start, end) ->
                      new ReplaceJSONBValuePart(
                        "Replace " + targetField.toString() + " on " + label,
                        targetField,
                        condition,
                        field("'[]'::jsonb", JSONB.class)
                      )
                  )
                )
                .toList()
            )
      )
    );
  }
}
