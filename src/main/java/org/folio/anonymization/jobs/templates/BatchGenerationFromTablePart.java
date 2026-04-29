package org.folio.anonymization.jobs.templates;

import java.util.ArrayList;
import java.util.List;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.db.TableIDs;
import org.folio.anonymization.domain.db.TableReference;
import org.folio.anonymization.domain.job.BatchPartFactory;
import org.folio.anonymization.domain.job.JobPart;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Table;

/**
 * Job part to create a series of batch processing children, each acting on a range of a table.
 *
 * The range for the batch will come from the table's values, based on a COUNT(1) query and then
 * successive paginated queries.
 */
@Log4j2
public class BatchGenerationFromTablePart<T> extends JobPart {

  private final FieldReference tableId;
  private final Class<T> tableIdType;
  private final int batchSize;
  private final String childrenJobStage;
  private final BatchPartFactory factory;

  public BatchGenerationFromTablePart(
    String label,
    FieldReference tableId,
    Class<T> tableIdType,
    int batchSize,
    String childrenJobStage,
    BatchPartFactory factory
  ) {
    super(label);
    this.tableId = tableId;
    this.tableIdType = tableIdType;
    this.batchSize = batchSize;
    this.childrenJobStage = childrenJobStage;
    this.factory = factory;
  }

  @SuppressWarnings("unchecked")
  public BatchGenerationFromTablePart(
    String label,
    FieldReference otherField,
    int batchSize,
    String childrenJobStage,
    BatchPartFactory factory
  ) {
    this(
      label,
      TableIDs.getIdFor(otherField).getLeft(),
      (Class<T>) TableIDs.getIdFor(otherField).getRight(),
      batchSize,
      childrenJobStage,
      factory
    );
  }

  @SuppressWarnings("unchecked")
  public BatchGenerationFromTablePart(
    String label,
    TableReference table,
    int batchSize,
    String childrenJobStage,
    BatchPartFactory factory
  ) {
    this(
      label,
      TableIDs.getIdFor(table).getLeft(),
      (Class<T>) TableIDs.getIdFor(table).getRight(),
      batchSize,
      childrenJobStage,
      factory
    );
  }

  @Override
  protected void execute() {
    Table<?> table = tableId.table(this.tenant());
    int totalCount = this.create().fetchCount(table);
    log.info("Total count: {}", totalCount);

    Field<T> field = this.tableId.field(this.tenant(), this.tableIdType);

    // get the value of every batchSize'th element in the table
    List<T> values = new ArrayList<>();

    for (int offset = 0; offset < totalCount; offset += batchSize) {
      values.add(this.create().select(field).from(table).orderBy(field).limit(1).offset(offset).fetchOne(field));
    }

    for (int i = 0; i < values.size(); i++) {
      T startValueInclusive = values.get(i);
      String prettyEndValue = "end";
      Condition condition = field.greaterOrEqual(startValueInclusive);
      if (i + 1 < values.size()) {
        T endValueExclusive = values.get(i + 1);
        prettyEndValue = endValueExclusive.toString();
        condition = condition.and(field.lessThan(endValueExclusive));
      }

      this.job.scheduleParts(
          childrenJobStage,
          List.of(
            factory.build(
              "(%s to %s)".formatted(i == 0 ? "start" : startValueInclusive.toString(), prettyEndValue),
              condition,
              i * batchSize,
              Math.min((i + 1) * batchSize, totalCount)
            )
          )
        );
    }
  }
}
