package org.folio.anonymization.util;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.trueCondition;

import lombok.experimental.UtilityClass;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.folio.Tenant;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.jooq.Condition;
import org.jooq.JSONB;

@UtilityClass
public class SystemUserExclusionUtil {

  public static Condition getExclusionCondition(FieldReference field, TenantExecutionContext tenant) {
    return getExclusionCondition(field, tenant.tenant());
  }

  public static Condition getExclusionCondition(FieldReference field, Tenant tenant) {
    if (field.schema().equals("users") && field.table().equals("users")) {
      return getExclusionCondition(tenant);
    }

    return trueCondition();
  }

  public static Condition getExclusionCondition(Tenant tenant) {
    return field(
      "COALESCE({0}->>'type', '')",
      String.class,
      new FieldReference("users", "users", "jsonb").baseColumn(tenant, JSONB.class)
    )
      .ne("system");
  }
}
