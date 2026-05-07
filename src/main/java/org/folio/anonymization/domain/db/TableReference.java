package org.folio.anonymization.domain.db;

import lombok.With;
import org.folio.anonymization.domain.folio.Tenant;
import org.folio.anonymization.util.DBUtils;
import org.jooq.Table;
import org.jooq.impl.DSL;

/**
 * Defines a table's location (without the tenant_mod_ prefix). For example:
 *
 * @example
 * new TableReference("fqm_manager", "entity_type_definition");
 */
@With
public record TableReference(String schema, String table) {
  public String toString() {
    return String.format("%s.%s", schema, table);
  }

  public Table<?> table(Tenant tenant) {
    return DSL.table(DSL.name(DBUtils.getSchemaName(tenant.id(), schema), table));
  }

  public FieldReference field(String column) {
    return new FieldReference(schema, table, column);
  }

  public FieldReference field(String column, String jsonPath) {
    return new FieldReference(schema, table, column, jsonPath);
  }
}
