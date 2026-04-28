package org.folio.anonymization.jobs.templates;

import org.folio.anonymization.domain.db.TableReference;
import org.folio.anonymization.domain.job.JobPart;

/**
 * Job part to truncate an entire table.
 */
public class TruncateTablePart extends JobPart {

  private final TableReference table;

  public TruncateTablePart(TableReference table) {
    super("truncate %s".formatted(table.toString()));
    this.table = table;
  }

  @Override
  protected void execute() {
    this.create().truncate(table.table(this.tenant())).execute();
  }
}
