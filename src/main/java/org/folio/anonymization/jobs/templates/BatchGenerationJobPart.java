package org.folio.anonymization.jobs.templates;

import java.util.List;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.function.TriFunction;
import org.folio.anonymization.domain.job.JobPart;
import org.folio.anonymization.util.DBUtils;
import org.jooq.Condition;
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
public class BatchGenerationJobPart extends JobPart {

  private final Table<?> table;
  private final int batchSize;
  private final String childrenJobStage;
  private final BatchPartFactory factory;

  public BatchGenerationJobPart(
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
            factory.apply(
              sequenceField.greaterOrEqual(minInclusive).and(sequenceField.lessThan(maxExclusive)),
              minInclusive,
              Math.min(maxExclusive - 1, totalMaxExclusive - 1)
            )
          )
        );
    }
  }

  /**
   * Factory for creating parts based on a batch range. Given a jOOQ condition to match the range and an
   * integer to start with (based on the range). It is guaranteed that this value on the range of
   * [value, value + count(condition)) is exclusive to this part and can be used as the basis for unique
   * values. The second value is the maximum for convenience, equal to min(value + batch_size, total)
   */
  public static interface BatchPartFactory extends TriFunction<Condition, Integer, Integer, JobPart> {}
}
