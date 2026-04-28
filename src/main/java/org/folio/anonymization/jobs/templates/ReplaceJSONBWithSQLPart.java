package org.folio.anonymization.jobs.templates;

import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.job.JobPart;

/**
 * Job part to replace a JSONB field with an arbitrary SQL value. Note that the replacementSql must be
 * a valid JSONB expression.
 *
 * @example
 * new ReplaceJSONBWithSQLPart("hardcode value", field, "\"hardcoded_value\"::jsonb")
 */
public class ReplaceJSONBWithSQLPart extends JobPart {

  private final FieldReference field;
  private final String replacementSql;

  public ReplaceJSONBWithSQLPart(String label, FieldReference field, String replacementSql) {
    super(label + " (" + field.toString() + ")");
    this.field = field;
    this.replacementSql = replacementSql;
  }

  @Override
  protected void execute() {
    this.create()
      .update(field.table(this.tenant()))
      .set(field.column(this.tenant()), field.jsonbSet(this.tenant(), replacementSql))
      .execute();
  }
}
