package org.folio.anonymization.jobs.templates;

import org.folio.anonymization.domain.job.JobPart;
import org.jooq.DSLContext;
import org.jooq.Table;

/**
 * Job part to truncate an entire table (jOOQ style).
 */
public class TruncateJooqTablePart extends JobPart {

  private final DSLContext create;
  private final Table<?> table;

  public TruncateJooqTablePart(String label, DSLContext create, Table<?> table) {
    super("truncate %s".formatted(label));
    this.create = create;
    this.table = table;
  }

  @Override
  protected void execute() {
    this.create.truncate(table).cascade().execute();
  }
}
