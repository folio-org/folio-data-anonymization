package org.folio.anonymization.jobs.templates;

import static org.jooq.impl.DSL.noCondition;

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

  public ReplaceJSONBValuePart(String label, FieldReference field, Field<JSONB> replacement) {
    this(label, field, f -> replacement);
  }

  public ReplaceJSONBValuePart(
    String label,
    FieldReference field,
    Function<Field<JSONB>, Field<JSONB>> getReplacement
  ) {
    this(label, field, getReplacement, noCondition());
  }

  public ReplaceJSONBValuePart(
    String label,
    FieldReference field,
    Function<Field<JSONB>, Field<JSONB>> getReplacement,
    Condition condition
  ) {
    super(label + " (" + field.toString() + ")");
    this.field = field;
    this.replacement = getReplacement;
    this.condition = condition;
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
