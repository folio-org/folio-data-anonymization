package org.folio.anonymization.jobs;

import static org.jooq.impl.DSL.trueCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.folio.anonymization.domain.db.ModuleTable;
import org.folio.anonymization.domain.folio.Tenant;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobPart;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.BatchGenerationFromTablePart;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;

class BulkOperationsAndListsAnonymizationTest {

  private static final Tenant TEST_TENANT = new Tenant("test", "Test", "Test tenant", null, false);

  @Test
  void buildSchedulesAllPartsWhenTablesExist() throws Exception {
    BulkOperationsAndListsAnonymization anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(
      TEST_TENANT,
      List.of(
        new ModuleTable("bulk_operations", "bulk_operation", 10),
        new ModuleTable("bulk_operations", "bulk_operation_execution_content", 10),
        new ModuleTable("bulk_operations", "bulk_operation_rule_details", 10),
        new ModuleTable("bulk_operations", "profile", 10),
        new ModuleTable("lists", "list_details", 10),
        new ModuleTable("lists", "list_versions", 10)
      )
    );

    Job job = anonymization.getBuilders(tenant).getFirst().build();
    ConcurrentLinkedQueue<?> prepareParts = job.getParts().get("prepare");
    assertNotNull(prepareParts);
    assertEquals(7, prepareParts.size());

    List<JobPart> generatedOverwriteParts = prepareParts
      .stream()
      .map(BatchGenerationFromTablePart.class::cast)
      .map(BatchGenerationFromTablePart::getFactory)
      .map(factory -> factory.build("", trueCondition(), 0, 1))
      .toList();

    assertEquals(7, generatedOverwriteParts.size());
    assertTrue(generatedOverwriteParts.stream().anyMatch(part -> part.getLabel().contains("bulk_operation.fql_query")));
    assertTrue(generatedOverwriteParts.stream().anyMatch(part -> part.getLabel().contains("list_details.fql_query")));
    assertTrue(generatedOverwriteParts.stream().anyMatch(part -> part.getLabel().contains("list_versions.fql_query")));
    assertTrue(generatedOverwriteParts.stream().anyMatch(part -> part.getLabel().contains("identifier")));
    assertTrue(generatedOverwriteParts.stream().anyMatch(part -> part.getLabel().contains("initial_value")));
    assertTrue(generatedOverwriteParts.stream().anyMatch(part -> part.getLabel().contains("updated_value")));
    assertTrue(generatedOverwriteParts.stream().anyMatch(part -> part.getLabel().contains("entity_type=USER")));
  }

  @Test
  void buildSkipsPartsWhenTablesMissing() throws Exception {
    BulkOperationsAndListsAnonymization anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(
      TEST_TENANT,
      List.of(new ModuleTable("users", "outbox_event_log", 10))
    );

    Job job = anonymization.getBuilders(tenant).getFirst().build();
    assertTrue(job.getParts().getOrDefault("prepare", new ConcurrentLinkedQueue<>()).isEmpty());
  }

  private static BulkOperationsAndListsAnonymization createFactoryWithContext() throws Exception {
    BulkOperationsAndListsAnonymization anonymization = new BulkOperationsAndListsAnonymization();
    Field contextField = BulkOperationsAndListsAnonymization.class.getDeclaredField("context");
    contextField.setAccessible(true);
    contextField.set(anonymization, new SharedExecutionContext((DSLContext) null, Runnable::run));
    return anonymization;
  }
}
