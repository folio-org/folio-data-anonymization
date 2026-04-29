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

class AddressAnonymizationTest {

  private static final Tenant TEST_TENANT = new Tenant("test", "Test", "Test tenant", null, false);

  @Test
  void buildSchedulesAddressReplacementBatchParts() throws Exception {
    Job job = buildJobWithTables(new ModuleTable("users", "users", 100));
    List<BatchGenerationFromTablePart<?>> parts = getPrepareParts(job);
    assertEquals(6, parts.size());

    assertHasLabel(parts, "$.personal.addresses[*].addressLine1");
    assertHasLabel(parts, "$.personal.addresses[*].addressLine2");
    assertHasLabel(parts, "$.personal.addresses[*].city");
    assertHasLabel(parts, "$.personal.addresses[*].region");
    assertHasLabel(parts, "$.personal.addresses[*].postalCode");
    assertHasLabel(parts, "$.personal.addresses[*].countryId");
  }

  @Test
  void buildDoesNotSchedulePartsWhenUsersTableIsMissing() throws Exception {
    Job job = buildJobWithTables(new ModuleTable("users", "outbox_event_log", 10));
    assertTrue(getPrepareParts(job).isEmpty());
  }

  private static Job buildJobWithTables(ModuleTable... tables) throws Exception {
    AddressAnonymization anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(TEST_TENANT, List.of(tables));
    return anonymization.getBuilders(tenant).getFirst().build();
  }

  private static AddressAnonymization createFactoryWithContext() throws Exception {
    AddressAnonymization anonymization = new AddressAnonymization();
    Field contextField = AddressAnonymization.class.getDeclaredField("context");
    contextField.setAccessible(true);
    contextField.set(anonymization, new SharedExecutionContext((DSLContext) null, job -> {}, Runnable::run));
    return anonymization;
  }

  private static List<BatchGenerationFromTablePart<?>> getPrepareParts(Job job) {
    ConcurrentLinkedQueue<?> prepareParts = job.getParts().get("prepare");
    if (prepareParts == null) {
      return List.of();
    }
    List<BatchGenerationFromTablePart<?>> parts = prepareParts
      .stream()
      .map(part -> (BatchGenerationFromTablePart<?>) part)
      .toList();
    assertNotNull(parts);
    return parts;
  }

  private static void assertHasLabel(List<BatchGenerationFromTablePart<?>> parts, String jsonPath) {
    String fullPathToken = "->'" + jsonPath + "'";
    assertTrue(parts.stream().anyMatch(part -> part.getLabel().contains(fullPathToken)));
  }
}
