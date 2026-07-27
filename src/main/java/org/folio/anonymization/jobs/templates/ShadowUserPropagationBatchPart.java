package org.folio.anonymization.jobs.templates;

import static org.jooq.impl.DSL.field;

import java.util.UUID;
import org.folio.anonymization.config.JobConfig;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.folio.Tenant;
import org.jooq.JSONB;

/**
 * Job part to make batches for shadow user data synchronization from one temporary table to another within a consortia.
 *
 * The `tempTableTemplate` must point to a table in `public` that is present for both the current
 * and sibling tenant.
 *
 * @example
 * new ShadowUserPropagationBatchPart("propagate", tenant, sibling, field, "_danon_%s_user_external_system_ids", "propagate-shadow-users")
 */
public class ShadowUserPropagationBatchPart extends BatchGenerationFromTablePart<UUID> {

  public ShadowUserPropagationBatchPart(
    String label,
    Tenant baseTenant,
    Tenant sibling,
    String jsonbProperty,
    String tempTableTemplate,
    String stage
  ) {
    // pull users from users table where type = 'shadow' for batches
    super(
      "Make batches to propagate shadow user data from " + sibling.id(),
      new FieldReference("users", "users", "id"),
      UUID.class,
      JobConfig.BATCH_SIZE / 10, // we are reading these in to do multiple subsequent queries with explicit
      stage,
      // copy data from the sibling's temp table to the current tenant's temp table
      (l, condition, start, end) ->
        new ShadowUserPropagationPart(
          "Propagate shadow user data from " + sibling.id() + " into " + tempTableTemplate + " on " + l,
          sibling,
          jsonbProperty,
          tempTableTemplate,
          condition
        ),
      field(
        "COALESCE({0}->>'type', '')",
        String.class,
        new FieldReference("users", "users", "jsonb").baseColumn(baseTenant, JSONB.class)
      )
        .equal("shadow")
      // We cannot rely on `originaltenantid` being properly filled...
      // .and(
      //   field(
      //     "COALESCE({0}->'customFields'->>'originaltenantid', '')",
      //     String.class,
      //     new FieldReference("users", "users", "jsonb").baseColumn(baseTenant, JSONB.class)
      //   )
      //     .equalIgnoreCase(sibling.id())
      // )
    );
  }
}
