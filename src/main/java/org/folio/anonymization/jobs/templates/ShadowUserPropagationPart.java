package org.folio.anonymization.jobs.templates;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.inline;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.table;

import java.util.UUID;
import java.util.function.UnaryOperator;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.folio.Tenant;
import org.folio.anonymization.domain.job.JobPart;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Table;
import org.jooq.impl.SQLDataType;

/**
 * Job part to sync shadow user's data from one temporary table to another within a consortia.
 *
 * The `tempTableTemplate` must point to a table in `public` that is present for both the current
 * and sibling tenant.
 *
 * `condition` will refer to a range of user IDs to propagate and MAY contain users which are not
 * from the sibling tenant. The part will only propagate users which are truly from the sibling tenant (based on UUID).
 *
 * The `jsonbProperty` is the property in the `users.jsonb` column which the mapping will actually use. This is
 * all that is supported at this time.
 *
 * @example
 * new ShadowUserPropagationPart("propagate", sibling, "username", "_danon_%s_user_external_system_ids", condition, UnaryOperator.identity())
 */
@Log4j2
public class ShadowUserPropagationPart extends JobPart {

  private final Tenant sibling;
  private final String jsonbProperty;
  private final String tempTableTemplate;
  private final Condition userCondition;
  private final UnaryOperator<Field<String>> valueTransformer;

  public ShadowUserPropagationPart(
    String label,
    Tenant sibling,
    String jsonbProperty,
    String tempTableTemplate,
    Condition condition,
    UnaryOperator<Field<String>> valueTransformer
  ) {
    super(label);
    this.sibling = sibling;
    this.jsonbProperty = jsonbProperty;
    this.tempTableTemplate = tempTableTemplate;
    this.userCondition = condition;
    this.valueTransformer = valueTransformer;
  }

  @Override
  protected void execute() {
    Table<?> ourTempTable = table(name("public", this.tempTableTemplate.formatted(tenant().id())));
    Table<?> siblingTempTable = table(name("public", this.tempTableTemplate.formatted(sibling.id())));

    Field<String> rawOriginalValue = field("original_value", SQLDataType.VARCHAR.notNull());
    Field<String> rawNewValue = field("new_value", SQLDataType.VARCHAR.null_());
    Field<String> siblingOriginalValue = field(
      siblingTempTable.getQualifiedName().append("original_value"),
      SQLDataType.VARCHAR.notNull()
    );
    Field<String> siblingNewValue = valueTransformer.apply(
      field(siblingTempTable.getQualifiedName().append("new_value"), SQLDataType.VARCHAR.null_())
    );

    FieldReference userIdReference = new FieldReference("users", "users", "id");
    Table<?> ourUsersTable = userIdReference.table(tenant());
    Table<?> siblingUsersTable = userIdReference.table(sibling);

    Field<UUID> ourUserIdField = userIdReference.baseColumn(tenant(), UUID.class);
    Field<UUID> siblingUserIdField = userIdReference.baseColumn(sibling, UUID.class);

    FieldReference userJsonbReference = new FieldReference("users", "users", "jsonb");
    Field<String> siblingUserType = field(
      "{0}->>'type'",
      String.class,
      userJsonbReference.baseColumn(sibling, JSONB.class)
    );

    Field<String> ourValueField = field(
      "{0}->>{1}",
      String.class,
      userJsonbReference.baseColumn(tenant(), JSONB.class),
      inline(this.jsonbProperty)
    );
    Field<String> siblingValueField = field(
      "{0}->>{1}",
      String.class,
      userJsonbReference.baseColumn(sibling, JSONB.class),
      inline(this.jsonbProperty)
    );

    this.create()
      .insertInto(ourTempTable)
      .columns(rawOriginalValue, rawNewValue)
      .select(
        select(ourValueField.as(rawOriginalValue), siblingNewValue.as(rawNewValue))
          .from(ourUsersTable)
          .join(siblingUsersTable)
          .on(ourUserIdField.eq(siblingUserIdField))
          .join(siblingTempTable)
          .on(siblingValueField.eq(siblingOriginalValue))
          .where(this.userCondition.and(siblingUserType.ne("shadow")).and(ourValueField.isNotNull()))
      )
      .onConflict(rawOriginalValue)
      .doUpdate()
      .set(rawNewValue, field("EXCLUDED.new_value", String.class))
      .execute();
  }
}
