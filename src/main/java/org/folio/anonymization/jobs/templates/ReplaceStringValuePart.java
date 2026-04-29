package org.folio.anonymization.jobs.templates;

import lombok.Getter;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.job.JobPart;
import org.jooq.Field;

/**
 * Job part to replace a non-JSON text field with an arbitrary SQL text value.
 */
public class ReplaceStringValuePart extends JobPart {

  private final FieldReference field;

  @Getter // for testing
  private final Field<String> replacement;

  public ReplaceStringValuePart(String label, FieldReference field, Field<String> replacement) {
    super(label + " (" + field.toString() + ")");
    this.field = field;
    this.replacement = replacement;
  }

  @Override
  protected void execute() {
    this.create()
      .update(field.table(this.tenant()))
      .set(field.baseColumn(this.tenant(), String.class), replacement)
      .execute();
  }
}
