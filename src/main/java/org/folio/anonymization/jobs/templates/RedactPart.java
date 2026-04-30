package org.folio.anonymization.jobs.templates;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.translate;

import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.job.JobPart;
import org.folio.anonymization.util.DBUtils;
import org.folio.anonymization.util.RandomValueUtils;
import org.jooq.Condition;
import org.jooq.JSONB;

/**
 * Job part to redact all alphanumeric characters in a field, JSONB or plain, using the TRUNCATE
 * function on the unaccented value.
 *
 * @example
 * new RedactPart("redact value", field, condition)
 */
public class RedactPart extends JobPart {

  private final FieldReference field;
  private final Condition condition;

  public RedactPart(String label, FieldReference field, Condition condition) {
    super(label);
    this.field = field;
    this.condition = condition;
  }

  @Override
  protected void execute() {
    if (field.jsonPath() != null) {
      this.create()
        .update(field.table(this.tenant()))
        .set(
          field.baseColumn(this.tenant()),
          field.jsonbSet(
            this.tenant(),
            innerField ->
              field(
                "to_jsonb({0})",
                JSONB.class,
                translate(
                  field("unaccent({0})", String.class, DBUtils.jsonbToString(innerField)),
                  RandomValueUtils.POSTGRES_TRANSLATE_FROM,
                  RandomValueUtils.POSTGRES_TRANSLATE_TO
                )
              )
          )
        )
        .where(condition)
        .execute();
    } else {
      this.create()
        .update(field.table(this.tenant()))
        .set(
          field.baseColumn(this.tenant()),
          translate(
            field("unaccent({0})", String.class, field.baseColumn(this.tenant())),
            RandomValueUtils.POSTGRES_TRANSLATE_FROM,
            RandomValueUtils.POSTGRES_TRANSLATE_TO
          )
        )
        .where(field.baseColumn(this.tenant()).isNotNull().and(this.condition))
        .execute();
    }
  }
}
