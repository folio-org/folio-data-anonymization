package org.folio.anonymization.jobs.templates;

import java.util.function.Function;
import lombok.Getter;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.job.JobPart;
import org.jooq.Condition;
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
  private final Condition condition;

  @Getter // for testing
  private final Function<Field<JSONB>, Field<JSONB>> replacement;

  public ReplaceJSONBValuePart(
    String label,
    FieldReference field,
    Condition condition,
    Function<Field<JSONB>, Field<JSONB>> getReplacement
  ) {
    super(label);
    this.field = field;
    this.replacement = getReplacement;
    this.condition = condition;
  }

  public ReplaceJSONBValuePart(String label, FieldReference field, Condition condition, Field<JSONB> replacement) {
    this(label, field, condition, i -> replacement);
  }

  @Override
  protected void execute() {
    this.create()
      .update(field.table(this.tenant()))
      .set(field.baseColumn(this.tenant()), field.jsonbSet(this.tenant(), replacement))
      .where(condition)
      .execute();
  }
}
