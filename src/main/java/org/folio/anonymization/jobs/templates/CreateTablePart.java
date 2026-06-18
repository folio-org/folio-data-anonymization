package org.folio.anonymization.jobs.templates;

import java.util.ArrayList;
import java.util.List;
import org.folio.anonymization.domain.job.JobPart;
import org.folio.anonymization.util.DBUtils;
import org.jooq.Constraint;
import org.jooq.CreateTableAsStep;
import org.jooq.Field;
import org.jooq.Select;
import org.jooq.Sequence;
import org.jooq.Table;

/**
 * Job part to create a table. Note the following caveats:
 * - This table MUST be manually destroyed later
 * - This table MUST have a primary key specified
 * - This table SHOULD have a tenant name prepended, if applicable
 * - If a table with this name already exists, this part will drop and replace it
 * - If a sequence is requested, a column _seq will be added at the end of the list that is an integer incrementing from 0,
 *     and a corresponding {table}_seq sequence will be created
 */
public class CreateTablePart extends JobPart {

  private final Table<?> table;
  private final List<Field<?>> fields;
  private final List<Constraint> constraints;
  private final boolean includeSequence;
  private Select<?> as;

  public CreateTablePart(
    String label,
    Table<?> table,
    List<Field<?>> fields,
    List<Constraint> constraints,
    boolean includeSequence,
    Select<?> as
  ) {
    super(label);
    this.table = table;
    this.fields = new ArrayList<>(fields);
    this.constraints = constraints;
    this.includeSequence = includeSequence;
    this.as = as;
  }

  public CreateTablePart(
    String label,
    Table<?> table,
    List<Field<?>> fields,
    List<Constraint> constraints,
    boolean includeSequence
  ) {
    this(label, table, fields, constraints, includeSequence, null);
  }

  @Override
  protected void execute() {
    Sequence<Integer> sequence = DBUtils.getSequence(table);

    this.create().dropTableIfExists(table).cascade().execute();

    if (includeSequence) {
      this.create().dropSequenceIfExists(sequence).execute();
      this.create().createSequence(sequence).startWith(0).minvalue(0).execute();
    }

    CreateTableAsStep query =
      this.create()
        .createTable(table)
        .columns(fields)
        .columns(includeSequence ? new Field[] { DBUtils.getSequenceField(table) } : new Field[] {})
        .constraints(constraints);

    if (this.as != null) {
      query.as(this.as).execute();
    } else {
      query.execute();
    }
  }
}
