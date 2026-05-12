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
import org.junit.jupiter.api.Test;

class FirstNameAnonymizationTest {

  private static final Tenant TEST_TENANT = new Tenant("test", "Test", "Test tenant", null, false);

  @Test
  void buildSchedulesFirstNameReplacementBatchParts() throws Exception {
    Job job = buildJobWithTables(
      new ModuleTable("users", "users", 100),
      new ModuleTable("users", "staging_users", 100)
    );
    List<? extends BatchGenerationFromTablePart<?>> parts = getPrepareParts(job);
    assertEquals(4, parts.size());

    assertHasLabel(parts, "$.personal.firstName");
    assertHasLabel(parts, "$.personal.preferredFirstName");
    assertHasLabel(parts, "$.generalInfo.firstName");
    assertHasLabel(parts, "$.generalInfo.preferredFirstName");
  }

  @Test
  void buildDoesNotSchedulePartsWhenUsersTablesAreMissing() throws Exception {
    Job job = buildJobWithTables(new ModuleTable("users", "outbox_event_log", 10));
    assertTrue(getPrepareParts(job).isEmpty());
  }

  private static Job buildJobWithTables(ModuleTable... tables) throws Exception {
    FirstNameAnonymization anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(TEST_TENANT, List.of(tables));
    return anonymization.getBuilders(tenant).getFirst().build();
  }

  private static FirstNameAnonymization createFactoryWithContext() throws Exception {
    FirstNameAnonymization anonymization = new FirstNameAnonymization();
    Field contextField = FirstNameAnonymization.class.getDeclaredField("context");
    contextField.setAccessible(true);
    contextField.set(anonymization, SharedExecutionContext.forTests());
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

  private static void assertHasLabel(List<? extends BatchGenerationFromTablePart<?>> parts, String jsonPath) {
    String fullPathToken = "->'" + jsonPath + "'";
    assertTrue(parts.stream().anyMatch(part -> part.getLabel().contains(fullPathToken)));
  }
}
