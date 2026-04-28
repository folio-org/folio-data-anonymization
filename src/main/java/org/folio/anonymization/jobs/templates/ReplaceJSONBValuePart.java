package org.folio.anonymization.jobs.templates;

import lombok.Getter;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.job.JobPart;
import org.jooq.Field;
import org.jooq.JSONB;

/**
 * Job part to replace a JSONB field with an arbitrary value.
 *
 * @example
 * new ReplaceJSONBValuePart("hardcode value", field, field("\"hardcoded_value\"::jsonb", JSONB.class))
 */
public class ReplaceJSONBValuePart extends JobPart {

  private final FieldReference field;

  @Getter // for testing
  private final Field<JSONB> replacement;

  public ReplaceJSONBValuePart(String label, FieldReference field, Field<JSONB> replacement) {
    super(label + " (" + field.toString() + ")");
    this.field = field;
    this.replacement = replacement;
  }

  @Override
  protected void execute() {
    this.create()
      .update(field.table(this.tenant()))
      .set(field.column(this.tenant()), field.jsonbSet(this.tenant(), replacement))
      .execute();
  }
}
