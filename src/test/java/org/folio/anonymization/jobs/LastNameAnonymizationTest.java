package org.folio.anonymization.jobs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.folio.anonymization.domain.db.ModuleTable;
import org.folio.anonymization.domain.folio.Tenant;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.BatchGenerationFromTablePart;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;

class LastNameAnonymizationTest {

  private static final Tenant TEST_TENANT = new Tenant("test", "Test", "Test tenant", null, false);

  @Test
  void buildSchedulesLastNameReplacementBatchParts() throws Exception {
    Job job = buildJobWithTables(
      new ModuleTable("users", "users", 100),
      new ModuleTable("users", "staging_users", 100),
      new ModuleTable("requests_mediated", "mediated_request", 100)
    );
    List<? extends BatchGenerationFromTablePart<?>> parts = getPrepareParts(job);
    assertEquals(4, parts.size());

    assertHasLabel(parts, "$.personal.lastName");
    assertHasLabel(parts, "$.generalInfo.lastName");
    assertHasLabel(parts, ".requester_last_name");
    assertHasLabel(parts, ".proxy_last_name");
  }

  @Test
  void buildSchedulesInventoryStorageItemPartByDefault() throws Exception {
    Job job = buildJobWithTables(new ModuleTable("inventory_storage", "item", 100));
    assertTrue(
      getPrepareParts(job)
        .stream()
        .anyMatch(part -> part.getLabel().contains("$.circulationNotes[*].source.personal.lastName"))
    );
  }

  private static Job buildJobWithTables(ModuleTable... tables) throws Exception {
    LastNameAnonymization anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(TEST_TENANT, List.of(tables));
    return anonymization.getBuilders(tenant).getFirst().build();
  }

  private static LastNameAnonymization createFactoryWithContext() throws Exception {
    LastNameAnonymization anonymization = new LastNameAnonymization();
    Field contextField = LastNameAnonymization.class.getDeclaredField("context");
    contextField.setAccessible(true);
    contextField.set(anonymization, new SharedExecutionContext((DSLContext) null, job -> {}, Runnable::run));
    return anonymization;
  }

  private static List<? extends BatchGenerationFromTablePart<?>> getPrepareParts(Job job) {
    ConcurrentLinkedQueue<?> prepareParts = job.getParts().get("prepare");
    if (prepareParts == null) {
      return List.of();
    }
    List<? extends BatchGenerationFromTablePart<?>> parts = prepareParts
      .stream()
      .map(part -> (BatchGenerationFromTablePart<?>) part)
      .toList();
    assertNotNull(parts);
    return parts;
  }

  private static void assertHasLabel(List<? extends BatchGenerationFromTablePart<?>> parts, String token) {
    assertTrue(parts.stream().anyMatch(part -> part.getLabel().contains(token)));
  }
}
