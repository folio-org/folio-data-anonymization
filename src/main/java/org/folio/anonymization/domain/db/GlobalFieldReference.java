package org.folio.anonymization.domain.db;

import java.util.function.Function;
import lombok.With;
import org.folio.anonymization.domain.folio.Tenant;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Table;
import org.jooq.impl.DSL;

/**
 * Defines a field's location outside the regular tenant_mod_ schema format. For example:
 *
 * @example
 * new GlobalFieldReference("public", "tenants", "id");
 * new GlobalFieldReference("data_import_global", "queue_items", "id");
 */
@With
public class GlobalFieldReference extends FieldReference {

  public GlobalFieldReference(String schema, String table, String column) {
    super(schema, table, column, null);
  }

  @Override
  public String toString() {
    return String.format("global %s.%s.%s", this.schema(), this.table(), this.column());
  }

  @Override
  public Table<?> table(Tenant tenant) {
    return DSL.table(DSL.name(this.schema(), this.table()));
  }

  @Override
  public TableReference tableReference() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Field<Object> baseColumn(Tenant tenant) {
    return DSL.field(DSL.name(this.schema(), this.table(), this.column()));
  }

  @Override
  public <T> Field<T> baseColumn(Tenant tenant, Class<T> clazz) {
    return DSL.field(DSL.name(this.schema(), this.table(), this.column()), clazz);
  }

  @Override
  public <T> Field<T> field(Tenant tenant, Class<T> clazz) {
    return this.baseColumn(tenant, clazz);
  }

  @Override
  public Field<JSONB> jsonbSet(Tenant tenant, Function<Field<JSONB>, Field<JSONB>> replacement) {
    throw new UnsupportedOperationException("GlobalFieldReference does not support JSONB");
  }
}
