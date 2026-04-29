package org.folio.anonymization.jobs.templates;

import static org.jooq.impl.DSL.noCondition;

import java.util.function.Function;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.job.JobPart;
import org.jooq.Condition;
import org.jooq.Field;

/**
 * Job part to replace a base column with an arbitrary {@link Field Field}. Non-nested variant of
 * {@link ReplaceJSONBValuePart}.
 *
 * @example
 * new ReplaceValuePart("hardcode value", field, field("something_else", String.class))
 */
public class ReplaceValuePart extends JobPart {

  private final FieldReference field;
  private final Condition condition;

  private final Function<Field<?>, Field<?>> replacement;

  public ReplaceValuePart(String label, FieldReference field, Field<?> replacement) {
    this(label, field, f -> replacement);
  }

  public ReplaceValuePart(String label, FieldReference field, Function<Field<?>, Field<?>> getReplacement) {
    this(label, field, getReplacement, noCondition());
  }

  public ReplaceValuePart(
    String label,
    FieldReference field,
    Function<Field<?>, Field<?>> getReplacement,
    Condition condition
  ) {
    super(label + " (" + field.toString() + ")");
    this.field = field;
    this.replacement = getReplacement;
    this.condition = condition;

    if (field.jsonPath() != null) {
      throw new IllegalStateException("Cannot use ReplaceValuePart on JSONB property " + field.toString());
    }
  }

  @Override
  protected void execute() {
    this.create()
      .update(field.table(this.tenant()))
      .set(field.baseColumn(this.tenant()), replacement.apply(field.baseColumn(this.tenant())))
      .where(field.baseColumn(this.tenant()).isNotNull().and(this.condition))
      .execute();
  }
}
