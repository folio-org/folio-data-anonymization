package org.folio.anonymization.jobs;

import java.util.List;
import java.util.stream.IntStream;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class UniqueLabelRedaction implements JobFactory {

  private static final FieldReference CANCELLATION_REASON_NAME = new FieldReference(
    "circulation_storage",
    "cancellation_reason",
    "jsonb",
    "$.name"
  );
  private static final FieldReference CHECKLIST_ITEM_NAME = new FieldReference(
    "oa",
    "checklist_item_definition",
    "clid_name"
  );

  private static final List<FieldReference> FIELDS = List.of(CANCELLATION_REASON_NAME, CHECKLIST_ITEM_NAME);

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "free_text_unique_labels",
        "Free text redaction — unique labels",
        "Replaces cancellation reason names and checklist item names with unique anonymized labels.",
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
                    "Prepare to apply unique labels to " + targetField.toString(),
                    targetField,
                    JobConfig.BATCH_SIZE,
                    "overwrite",
                    (label, condition, start, end) ->
                      new ReplaceValueFromListPart(
                        "Replace " + targetField.toString() + " on " + label,
                        targetField,
                        condition,
                        uniqueLabels(targetField, start, end)
                      )
                  )
                )
                .toList()
            )
      )
    );
  }

  private static List<String> uniqueLabels(FieldReference targetField, int start, int end) {
    int size = Math.max(end - start, 1);
    long now = System.nanoTime() % 1_000; // should be sufficient to avoid collisions across runs
    String prefix = CANCELLATION_REASON_NAME.equals(targetField)
      ? "Anonymized (" + now + ") reason #"
      : "Anonymized (" + now + ") checklist #";
    return IntStream.range(0, size).mapToObj(i -> prefix + (start + i + 1)).toList();
  }
}
