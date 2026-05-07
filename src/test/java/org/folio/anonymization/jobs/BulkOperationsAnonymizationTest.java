package org.folio.anonymization.jobs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.folio.anonymization.domain.db.ModuleTable;
import org.folio.anonymization.domain.folio.Tenant;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;

class BulkOperationsAnonymizationTest {

  private static final Tenant TEST_TENANT = new Tenant("test", "Test", "Test tenant", null, false);

  @Test
  void buildSchedulesAllPartsWhenTablesExist() throws Exception {
    BulkOperationsAnonymization anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(
      TEST_TENANT,
      List.of(
        new ModuleTable("bulk_operations", "bulk_operation", 10),
        new ModuleTable("bulk_operations", "bulk_operation_execution_content", 10),
        new ModuleTable("bulk_operations", "bulk_operation_rule_details", 10),
        new ModuleTable("bulk_operations", "profile", 10)
      )
    );

    Job job = anonymization.getBuilders(tenant).getFirst().build();
    ConcurrentLinkedQueue<?> overwriteParts = job.getParts().get("overwrite");

    assertEquals(5, overwriteParts.size());
    assertTrue(overwriteParts.stream().anyMatch(part -> ((org.folio.anonymization.domain.job.JobPart) part).getLabel().contains("fql_query")));
    assertTrue(overwriteParts.stream().anyMatch(part -> ((org.folio.anonymization.domain.job.JobPart) part).getLabel().contains("identifier")));
    assertTrue(overwriteParts.stream().anyMatch(part -> ((org.folio.anonymization.domain.job.JobPart) part).getLabel().contains("initial_value")));
    assertTrue(overwriteParts.stream().anyMatch(part -> ((org.folio.anonymization.domain.job.JobPart) part).getLabel().contains("updated_value")));
    assertTrue(overwriteParts.stream().anyMatch(part -> ((org.folio.anonymization.domain.job.JobPart) part).getLabel().contains("entity_type=USER")));
  }

  @Test
  void buildSkipsPartsWhenTablesMissing() throws Exception {
    BulkOperationsAnonymization anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(
      TEST_TENANT,
      List.of(new ModuleTable("users", "outbox_event_log", 10))
    );

    Job job = anonymization.getBuilders(tenant).getFirst().build();
    assertTrue(job.getParts().getOrDefault("overwrite", new ConcurrentLinkedQueue<>()).isEmpty());
  }

  private static BulkOperationsAnonymization createFactoryWithContext() throws Exception {
    BulkOperationsAnonymization anonymization = new BulkOperationsAnonymization();
    Field contextField = BulkOperationsAnonymization.class.getDeclaredField("context");
    contextField.setAccessible(true);
    contextField.set(anonymization, new SharedExecutionContext((DSLContext) null, Runnable::run));
    return anonymization;
  }
}
