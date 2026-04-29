package org.folio.anonymization.jobs.templates;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.DSL.using;

import java.util.List;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.db.TableIDs;
import org.folio.anonymization.domain.job.JobPart;
import org.folio.anonymization.util.DBUtils;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Query;
import org.jooq.Record1;
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
public class ReplaceValueFromListPart extends JobPart {

  private static final int INSERT_BATCH_SIZE = 100;

  private final FieldReference field;
  private final Condition condition;

  private final List<String> replacements;

  public ReplaceValueFromListPart(String label, FieldReference field, Condition condition, List<String> replacements) {
    super(label);
    this.field = field;
    this.condition = condition;
    this.replacements = replacements;
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
        this.create().createSequence(replacementsSequence).startWith(0).minvalue(0).execute();

        Field<Integer> replacementsSequenceField = DBUtils.getSequenceField(tempTable);
        Field<String> valueField = field("value", String.class);

        ctx
          .createTemporaryTable(tempTable)
          .columns(replacementsSequenceField, valueField)
          .primaryKey(replacementsSequenceField)
          .onCommitDrop()
          .execute();

        // populate new temporary table
        List<Query> queries = replacements
          .stream()
          .map(value -> ctx.insertInto(tempTable).columns(valueField).values(value))
          .map(Query.class::cast)
          .toList();

        for (int i = 0; i < queries.size(); i += INSERT_BATCH_SIZE) {
          int end = Math.min(i + INSERT_BATCH_SIZE, queries.size());
          List<Query> batch = queries.subList(i, end);
          this.create().batch(batch).execute();
        }

        // we know all values will be in our table with indexes [0,max)
        int maxSequenceValueExclusive = this.create().select(replacementsSequence.nextval()).fetchOne().value1();

        Sequence<Integer> insertionSequence = DBUtils.getSequence(tempTable2RefOnly);
        this.create()
          .createSequence(insertionSequence)
          .startWith(0)
          .minvalue(0)
          .maxvalue(maxSequenceValueExclusive - 1)
          .cycle()
          .execute();

        Select<? extends Record1<?>> select = select(valueField)
          // must query the next value from the insertion sequence in the from clause
          // to ensure it only runs once per subquery execution
          .from(tempTable, select(insertionSequence.nextval().as("chosen_seq")))
          .where(
            replacementsSequenceField
              .eq(field("chosen_seq", Integer.class))
              // we must bind to the outer column in some way or this will not be re-executed for each update
              // (thanks postgres for cleverly optimizing! 🙃)
              .and(TableIDs.getIdFor(field, this.tenant()).isNotNull())
          );

        // do the actual update
        if (field.jsonPath() == null) {
          ctx
            .update(field.table(this.tenant()))
            .set(field.baseColumn(this.tenant()), select)
            .where(field.baseColumn(this.tenant()).isNotNull().and(this.condition))
            .execute();
        } else {
          ctx
            .update(field.table(this.tenant()))
            .set(
              field.baseColumn(this.tenant()),
              field.jsonbSet(this.tenant(), i -> field("to_jsonb(({0}))", JSONB.class, select))
            )
            .where(this.condition)
            .execute();
        }

        ctx.dropTemporaryTable(tempTable).cascade().execute();
        ctx.dropSequence(replacementsSequence).execute();
        ctx.dropSequence(insertionSequence).execute();
      });
  }
}
