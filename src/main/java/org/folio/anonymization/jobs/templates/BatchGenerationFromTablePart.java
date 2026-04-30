package org.folio.anonymization.jobs.templates;

import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.inline;
import static org.jooq.impl.DSL.orderBy;
import static org.jooq.impl.DSL.rowNumber;

import java.util.List;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.db.TableIDs;
import org.folio.anonymization.domain.db.TableReference;
import org.folio.anonymization.domain.job.BatchPartFactory;
import org.folio.anonymization.domain.job.JobPart;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Record2;
import org.jooq.Result;
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

  @Getter // for testing
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
    Field<T> field = this.tableId.field(this.tenant(), this.tableIdType);

    // derive batch boundaries in a single pass using row_number(), for O(size).
    // counting via window function also allows us to save an extra count(*)
    Field<Integer> rowNum = rowNumber().over(orderBy(field)).as("rn");
    Field<Integer> total = count().over().as("total");

    Table<?> numbered = this.create().select(field.as("v"), rowNum, total).from(table).asTable("numbered");

    @SuppressWarnings("unchecked")
    Field<T> v = (Field<T>) numbered.field("v");
    Field<Integer> rn = numbered.field("rn", Integer.class);
    Field<Integer> tot = numbered.field("total", Integer.class);

    Result<Record2<T, Integer>> rows =
      this.create()
        .select(v, tot)
        .from(numbered)
        .where(rn.minus(inline(1)).mod(inline(batchSize)).eq(0))
        .orderBy(v)
        .fetch();

    int totalCount = rows.isEmpty() ? 0 : rows.get(0).value2();
    log.info("Total count: {}", totalCount);

    List<T> values = rows.map(Record2::value1);

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
