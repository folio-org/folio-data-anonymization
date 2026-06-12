package org.folio.anonymization.jobs.templates;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.row;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.DSL.using;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.config.JobConfig;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.db.TableIDs;
import org.folio.anonymization.domain.db.TableReference;
import org.folio.anonymization.domain.job.JobPart;
import org.folio.anonymization.util.DBUtils;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.Select;
import org.jooq.Sequence;
import org.jooq.Table;

/**
 * Job part to replace a base column with a value from a dynamically generated set.
 * An external source will be called to generate the values used for insertion.
 *
 * This performs the following steps in a transaction, to keep everything in SQL and efficient as possible
 * while still supporting nested JSONB:
 * - Create a temporary table to hold the `replacements` and a unique sequence value
 * - Insert all replacements into this table
 * - Update the target table with the values from the replacements table using a second sequence
 * - Drop the temporary table and sequences
 *
 * Note:
 * - This does not guarantee that each insertion will present a unique value (nested arrays may cause replacements > values)
 * - This does not guarantee that each generated value will end up in the final result (not applicable rows may cause replacements < values)
 */
@Log4j2
public class ReplaceValueFromListPart extends JobPart {

  private final List<FieldReference> fields;
  private final Condition condition;

  @Getter // for testing
  private final List<List<?>> replacements;

  private List<Field<?>> valueFields;

  public ReplaceValueFromListPart(String label, FieldReference field, Condition condition, List<String> replacements) {
    this(label, List.of(field), condition, List.of(replacements), List.of(field("value", String.class)));
  }

  /**
   * Replace multiple values at once. {@code fields}, {@code replacements}, and {@code replacementFields} must have the same size.
   * Each inner column within {@code replacements} must also be the same size.
   *
   * @param label job label
   * @param fields list of fields to replace
   * @param condition condition to use (for batching)
   * @param replacements list of list of replacement values
   * @param valueFields list of temporary fields to use for replacements (eg {@code field("value", String.class)})
   */
  public ReplaceValueFromListPart(
    String label,
    List<FieldReference> fields,
    Condition condition,
    List<List<?>> replacements,
    List<Field<?>> valueFields
  ) {
    super(label);
    this.fields = fields;
    this.condition = condition;
    this.replacements = replacements;
    this.valueFields = valueFields;

    if (replacements.stream().anyMatch(List::isEmpty)) {
      throw new IllegalArgumentException("I can't replace values with nothing!");
    }

    if (fields.size() != replacements.size() || fields.size() != valueFields.size()) {
      throw new IllegalArgumentException("Fields, replacements, and valueFields must have the same size!");
    }

    TableReference table = fields.get(0).tableReference();
    if (fields.stream().anyMatch(f -> !f.tableReference().equals(table))) {
      throw new IllegalArgumentException("All fields must be from the same table!");
    }

    int numReplacementsAvailable = replacements.get(0).size();
    if (replacements.stream().anyMatch(r -> r.size() != numReplacementsAvailable)) {
      throw new IllegalArgumentException("All inner lists of replacements must have the same size!");
    }
  }

  @Override
  protected void execute() {
    this.create()
      .transaction(configuration -> {
        DSLContext ctx = using(configuration);

        Table<?> tempTable = table(name("_danon_replacements_" + System.nanoTime()));
        // used only for reference to get a unique sequence name, no table of this actually exists
        Table<?> tempTable2RefOnly = table(name("_danon_insertions_" + System.nanoTime()));

        Sequence<Integer> replacementsSequence = DBUtils.getSequence(tempTable);
        ctx.createSequence(replacementsSequence).startWith(0).minvalue(0).execute();

        Field<Integer> replacementsSequenceField = DBUtils.getSequenceField(tempTable);

        ctx
          .createTemporaryTable(tempTable)
          .columns(Stream.concat(Stream.of(replacementsSequenceField), valueFields.stream()).toList())
          .primaryKey(replacementsSequenceField)
          .onCommitDrop()
          .execute();

        // populate new temporary table
        List<Query> queries = IntStream
          .range(0, replacements.get(0).size())
          .mapToObj(i ->
            ctx.insertInto(tempTable).columns(valueFields).values(replacements.stream().map(r -> r.get(i)).toList())
          )
          .map(Query.class::cast)
          .toList();

        for (int i = 0; i < queries.size(); i += JobConfig.INSERT_BATCH_SIZE) {
          int end = Math.min(i + JobConfig.INSERT_BATCH_SIZE, queries.size());
          List<Query> batch = queries.subList(i, end);
          ctx.batch(batch).execute();
        }

        Sequence<Integer> insertionSequence = DBUtils.getSequence(tempTable2RefOnly);
        int replacementCount = replacements.get(0).size();
        if (replacementCount != 1) {
          ctx
            .createSequence(insertionSequence)
            .startWith(0)
            .minvalue(0)
            .maxvalue(replacementCount - 1)
            .cycle()
            .execute();
        }

        // using a single correlated row-valued scalar subquery guarantees that all SET columns come from the SAME replacements row,
        // and that nextval() is called exactly once per updated row
        List<Field<?>> selectExprs = new ArrayList<>(fields.size());
        List<Field<?>> setTargets = new ArrayList<>(fields.size());
        Condition updateCondition = this.condition;
        for (int i = 0; i < fields.size(); i++) {
          FieldReference field = fields.get(i);
          Field<?> rawValue = valueFields.get(i);
          setTargets.add(field.baseColumn(this.tenant()));
          if (field.jsonPath() == null) {
            selectExprs.add(rawValue);
            updateCondition = updateCondition.and(field.baseColumn(this.tenant()).isNotNull());
          } else {
            selectExprs.add(field.jsonbSet(this.tenant(), inner -> field("to_jsonb({0})", JSONB.class, rawValue)));
            updateCondition = updateCondition.and(field.jsonPathExists(this.tenant()));
          }
        }

        Select<? extends Record> sourceSelect;
        if (replacementCount == 1) {
          // Avoid creating a sequence with MINVALUE == MAXVALUE, which Postgres rejects.
          sourceSelect =
            select(selectExprs)
              .from(tempTable)
              .where(TableIDs.getIdFor(fields.get(0), this.tenant()).isNotNull())
              .limit(1);
        } else {
          sourceSelect =
            select(selectExprs)
              // must query the next value from the insertion sequence in the from clause
              // to ensure it only runs once per subquery execution
              .from(tempTable, select(insertionSequence.nextval().as("chosen_seq")))
              .where(
                replacementsSequenceField
                  .eq(field("chosen_seq", Integer.class))
                  // we must bind to the outer column in some way or this will not be re-executed for each update
                  // (thanks postgres for cleverly optimizing! 🙃)
                  .and(TableIDs.getIdFor(fields.get(0), this.tenant()).isNotNull())
              );
        }

        // do the actual update — row-value assignment guarantees one shared replacements row per target row
        ctx
          .update(fields.get(0).table(this.tenant()))
          .set(row(setTargets.toArray(new Field<?>[0])), sourceSelect)
          .where(updateCondition)
          .execute();

        ctx.dropTemporaryTable(tempTable).cascade().execute();
        ctx.dropSequence(replacementsSequence).execute();
        ctx.dropSequenceIfExists(insertionSequence).execute();
      });
  }
}
