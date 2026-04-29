package org.folio.anonymization.jobs.templates;

import org.folio.anonymization.domain.job.JobPart;
import org.folio.anonymization.util.DBUtils;
import org.jooq.Table;

/**
 * Job part to drop a table. Note the following caveats:
 * - This table MUST exist. The job will fail otherwise.
 * - This table will be dropped from the `public` schema and SHOULD have a tenant name prepended, if applicable.
 * - Child sequences (as created by CreateTablePart) will also be dropped
 * - DROP will be executed with CASCADE
 */
public class DropTablePart extends JobPart {

  private final Table<?> table;

  public DropTablePart(String label, Table<?> table) {
    super(label);
    this.table = table;
  }

  @Override
  protected void execute() {
    this.create().dropTable(table).cascade().execute();
    this.create().dropSequenceIfExists(DBUtils.getSequence(table)).execute();
  }
}
