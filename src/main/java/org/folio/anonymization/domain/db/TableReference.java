package org.folio.anonymization.domain.db;

import org.folio.anonymization.domain.folio.Tenant;
import org.folio.anonymization.repository.UtilRepository;
import org.jooq.Table;
import org.jooq.impl.DSL;

/**
 * Defines a table's location (without the tenant_mod_ prefix). For example:
 *
 * @example
 * new TableReference("fqm_manager", "entity_type_definition");
 */
public record TableReference(String schema, String table) {
  public String toString() {
    return String.format("%s.%s", schema, table);
  }

  public Table<?> table(Tenant tenant) {
    return DSL.table(DSL.name(UtilRepository.getSchemaName(tenant.id(), schema), table));
  }
}
