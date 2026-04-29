package org.folio.anonymization.repository;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.table;

import java.util.List;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.domain.db.ModuleTable;
import org.folio.anonymization.util.DBUtils;
import org.jooq.DSLContext;
import org.jooq.Table;
import org.jooq.impl.SQLDataType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Log4j2
@Repository
public class UtilRepository {

  private static final Table<?> INFORMATION_SCHEMA_SCHEMATA = table(name("information_schema", "schemata"));

  @Autowired
  private DSLContext create;

  public boolean doesSchemaExist(String schemaName) {
    return (
      create
        .selectOne()
        .from(INFORMATION_SCHEMA_SCHEMATA)
        .where(field("schema_name", String.class).eq(schemaName))
        .fetchOne() !=
      null
    );
  }

  public boolean doesSchemaExist(String tenantName, String normalizedModuleName) {
    return doesSchemaExist(DBUtils.getSchemaName(tenantName, normalizedModuleName));
  }

  /**
   * Get a list of table sizes in schemas starting with the given prefix. These are approximate
   * and based on the information found in pg_class, unless the table has not been ANALYZE or VACUUMed,
   * in which case a full COUNT(*) will occur.
   */
  public List<ModuleTable> getTablesSizesBySchemaPrefix(String prefix) {
    return create
      .select(field("table_schema"), field("table_name"), field("size"))
      .from(table(name("information_schema", "tables")))
      .leftJoin(
        select(
          field(name("c", "relkind")).as("relkind"),
          field(name("c", "relispartition")).as("relispartition"),
          field(name("n", "nspname")).as("scm"),
          field(name("c", "relname")).as("tbl"),
          field(name("c", "reltuples")).cast(SQLDataType.BIGINT).as("size")
        )
          .from(table("pg_class").as("c"))
          .join(table("pg_namespace").as("n"))
          .on(field(name("c", "relnamespace")).eq(field(name("n", "oid"))))
      )
      .on(field("table_schema").eq(field("scm")).and(field("table_name").eq(field("tbl"))))
      .where(
        field("table_schema")
          .startsWith(prefix)
          .and(field(name("relkind")).eq("r"))
          .and(field(name("relispartition")).eq(false))
      )
      .fetch()
      .stream()
      .map(record ->
        new ModuleTable(
          record.get("table_schema", String.class).substring(prefix.length()),
          record.get("table_name", String.class),
          record.get("size", Integer.class)
        )
      )
      .toList();
  }
}
