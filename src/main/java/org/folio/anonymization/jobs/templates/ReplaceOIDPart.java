package org.folio.anonymization.jobs.templates;

import static org.jooq.impl.DSL.exists;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.selectOne;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.DSL.using;
import static org.jooq.impl.DSL.val;

import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.folio.Tenant;
import org.folio.anonymization.domain.job.JobPart;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;

/**
 * Job part to replace a large object referenced by an OID column with a static value.
 *
 * @example
 * new ReplaceOIDPart("hardcode value", field, condition, "Removed by anonymization".getBytes())
 */
public class ReplaceOIDPart extends JobPart {

  private final FieldReference field;
  private final Condition condition;

  private final byte[] replacement;

  public ReplaceOIDPart(String label, FieldReference field, Condition condition, byte[] replacement) {
    super(label);
    this.field = field;
    this.condition = condition;
    this.replacement = replacement;
  }

  @Override
  protected void execute() {
    this.create()
      .transaction(configuration -> {
        DSLContext ctx = using(configuration);
        Condition condition = this.conditionForExistingLargeObjects(this.tenant());

        // FDs from lo_open will auto-close at the end of the transaction, so we don't have to worry about cleaning them up
        // 0x00020000 = INV_WRITE, from postgres source code
        ctx
          .select(
            field("lo_truncate(lo_open({0}, 0x00020000), 0)", Integer.class, this.field.baseColumn(this.tenant()))
          )
          .from(this.field.table(this.tenant()))
          .where(condition)
          .execute();

        ctx
          .select(
            field(
              "lo_put({0}, 0, {1})",
              String.class, // returns void, but jooq is happy enough with this
              this.field.baseColumn(this.tenant()),
              val(this.replacement, byte[].class)
            )
          )
          .from(this.field.table(this.tenant()))
          .where(condition)
          .execute();
      });
  }

  Condition conditionForExistingLargeObjects(Tenant tenant) {
    Field<Object> oidColumn = this.field.baseColumn(tenant);
    Table<?> largeObjectMetadata = table(name("pg_catalog", "pg_largeobject_metadata")).as("lo_metadata");
    Field<Object> largeObjectOid = field(name("lo_metadata", "oid"));

    return this.condition
      .and(oidColumn.isNotNull())
      .and(exists(selectOne().from(largeObjectMetadata).where(largeObjectOid.eq(oidColumn))));
  }
}
