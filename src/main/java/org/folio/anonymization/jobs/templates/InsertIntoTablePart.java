package org.folio.anonymization.jobs.templates;

import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.DSL.using;

import org.folio.anonymization.domain.job.JobPart;
import org.jooq.DSLContext;
import org.jooq.Select;
import org.jooq.Table;

/**
 * Job part to insert data into a table. Note the following caveats:
 * - This table for insertion MUST be present in the `public` schema and
 *   SHOULD have a tenant name prepended, if applicable
 * - Insertion order will be based on the destination table column definitions
 *   and mapped 1:1 with the provided fields.
 * - Conflicts will be handled with the 'DO NOTHING' strategy.
 */
public class InsertIntoTablePart extends JobPart {

  private final Table<?> destinationTable;
  private final Select<?> source;

  public InsertIntoTablePart(String label, Table<?> destinationTable, Select<?> source) {
    super(label);
    this.destinationTable = destinationTable;
    this.source = source;
  }

  @Override
  protected void execute() {
    // this mess creates a transaction which creates a temporary table, loads the data into it,
    // then finally loads the data into the actual destinationTable with an EXCLUSIVE lock.
    // this prevents deadlocks where multiple JobParts are inserting with conflicting primary keys,
    // and the temporary table keeps things a little more efficient.
    this.create()
      .transaction(configuration -> {
        DSLContext ctx = using(configuration);

        Table<?> tempTable = table(name("_danon_staging_" + System.nanoTime()));

        ctx.createTemporaryTable(tempTable).as(source).onCommitDrop().execute();

        ctx.execute("LOCK TABLE {0} IN EXCLUSIVE MODE", destinationTable);
        ctx.insertInto(destinationTable).select(ctx.selectFrom(tempTable)).onConflictDoNothing().execute();
      });
  }
}
