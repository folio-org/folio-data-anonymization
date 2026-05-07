package org.folio.anonymization.jobs.templates;

import org.folio.anonymization.domain.db.TableReference;
import org.folio.anonymization.domain.job.JobPart;
import org.jooq.Condition;

/**
 * Job part to delete part of a table.
 */
public class DeletePart extends JobPart {

  private final TableReference table;
  private final Condition condition;

  public DeletePart(String label, TableReference table, Condition condition) {
    super(label);
    this.table = table;
    this.condition = condition;
  }

  @Override
  protected void execute() {
    this.create().deleteFrom(table.table(this.tenant())).where(condition).execute();
  }
}
