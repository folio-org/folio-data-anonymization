package org.folio.anonymization.domain.db;

import org.folio.anonymization.domain.folio.Tenant;
import org.folio.anonymization.repository.UtilRepository;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Table;
import org.jooq.impl.DSL;

/**
 * Defines a field's location (without the tenant_mod_ prefix). For example:
 *
 * @example
 * new FieldReference("users", "users", "id", null);
 * new FieldReference("users", "users", "jsonb", "$.id");
 */
public record FieldReference(String schema, String table, String column, String jsonPath) {
  public FieldReference(String schema, String table, String column) {
    this(schema, table, column, null);
  }

  public String toString() {
    if (jsonPath == null) {
      return String.format("%s.%s.%s", schema, table, column);
    } else {
      return String.format("%s.%s.%s->'%s'", schema, table, column, jsonPath);
    }
  }

  public Table<?> table(Tenant tenant) {
    return DSL.table(DSL.name(UtilRepository.getSchemaName(tenant.id(), schema), table));
  }

  public Field<Object> column(Tenant tenant) {
    return DSL.field(DSL.name(UtilRepository.getSchemaName(tenant.id(), schema), table, column));
  }

  public Field<JSONB> jsonbSet(Tenant tenant, String replacementSqlValue) {
    if (jsonPath == null) {
      throw new UnsupportedOperationException("Cannot use jsonb_set for a non-JSONB field");
    }
    String[] parts = jsonPath.substring(2).split("\\.");
    return DSL.field(
      "jsonb_set({0}, {1}, %s, false)".formatted(replacementSqlValue),
      JSONB.class,
      column(tenant),
      parts
    );
  }
}
