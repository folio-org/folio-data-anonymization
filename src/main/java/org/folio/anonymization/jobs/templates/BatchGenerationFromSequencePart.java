package org.folio.anonymization.jobs.templates;

import java.util.List;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.domain.job.BatchPartFactory;
import org.folio.anonymization.domain.job.JobPart;
import org.folio.anonymization.util.DBUtils;
import org.jooq.Field;
import org.jooq.Table;

/**
 * Job part to create a series of batch processing children, each acting on a range of a table.
 *
 * The max value for the batch will come from the table's {@link sequence DBUtils#getSequence}.
 * Note that, in the case of row INSERT conflicts, this value may be higher than the true number
 * of rows.
 */
@Log4j2
public class BatchGenerationFromSequencePart extends JobPart {

  private final Table<?> table;
  private final int batchSize;
  private final String childrenJobStage;
  private final BatchPartFactory factory;

  public BatchGenerationFromSequencePart(
    String label,
    Table<?> table,
    int batchSize,
    String childrenJobStage,
    BatchPartFactory factory
  ) {
    super(label);
    this.table = table;
    this.batchSize = batchSize;
    this.childrenJobStage = childrenJobStage;
    this.factory = factory;
  }

  @Override
  protected void execute() {
    int totalMaxExclusive = this.create().select(DBUtils.getSequence(table).nextval()).fetchOne().value1();
    log.info("Next value from sequence: {}", totalMaxExclusive);

    Field<Integer> sequenceField = DBUtils.getSequenceField(table);
    for (int minInclusive = 0; minInclusive < totalMaxExclusive; minInclusive += batchSize) {
      int maxExclusive = minInclusive + batchSize;
      this.job.scheduleParts(
          childrenJobStage,
          List.of(
            factory.build(
              "(%s to %s)".formatted(minInclusive, Math.min(maxExclusive - 1, totalMaxExclusive - 1)),
              sequenceField.greaterOrEqual(minInclusive).and(sequenceField.lessThan(maxExclusive)),
              minInclusive,
              Math.min(maxExclusive - 1, totalMaxExclusive - 1)
            )
          )
        );
    }
  }
}
