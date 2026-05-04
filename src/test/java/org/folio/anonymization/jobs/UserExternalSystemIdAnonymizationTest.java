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
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.BatchGenerationFromTablePart;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;

class UserExternalSystemIdAnonymizationTest {

  private static final Tenant TEST_TENANT = new Tenant("test", "Test", "Test tenant", null, false);

  @Test
  void buildSchedulesExternalSystemIdReplacementBatchParts() throws Exception {
    Job job = buildJobWithTables(
      new ModuleTable("users", "users", 100),
      new ModuleTable("users", "staging_users", 100),
      new ModuleTable("users", "user_tenant", 100)
    );
    List<? extends BatchGenerationFromTablePart<?>> parts = getEnumeratePrepParts(job);
    assertEquals(3, parts.size());

    assertHasLabel(parts, "$.externalSystemId");
    assertHasLabel(parts, ".external_system_id");
  }

  @Test
  void buildDoesNotSchedulePartsWhenTargetTablesAreMissing() throws Exception {
    Job job = buildJobWithTables(new ModuleTable("users", "outbox_event_log", 10));
    assertTrue(getEnumeratePrepParts(job).isEmpty());
  }

  private static Job buildJobWithTables(ModuleTable... tables) throws Exception {
    UserExternalSystemIdAnonymization anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(TEST_TENANT, List.of(tables));
    Job job = anonymization.getBuilders(tenant).getFirst().build();
    List<JobConfigurationProperty> settings = job.getContext().settings();
    settings.stream().filter(s -> "drop-table".equals(s.getKey())).forEach(s -> s.setBooleanValue(false));
    return job;
  }

  private static UserExternalSystemIdAnonymization createFactoryWithContext() throws Exception {
    UserExternalSystemIdAnonymization anonymization = new UserExternalSystemIdAnonymization();
    Field contextField = UserExternalSystemIdAnonymization.class.getDeclaredField("context");
    contextField.setAccessible(true);
    contextField.set(anonymization, new SharedExecutionContext((DSLContext) null, job -> {}, Runnable::run));
    return anonymization;
  }

  private static List<? extends BatchGenerationFromTablePart<?>> getEnumeratePrepParts(Job job) {
    ConcurrentLinkedQueue<?> enumeratePrepParts = job.getParts().get("enumerate-prep");
    if (enumeratePrepParts == null) {
      return List.of();
    }
    List<? extends BatchGenerationFromTablePart<?>> parts = enumeratePrepParts
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
