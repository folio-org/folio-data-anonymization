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
import org.folio.anonymization.jobs.templates.BatchGenerationFromTablePart;
import org.junit.jupiter.api.Test;

class UniqueLabelRedactionTest {

  private static final Tenant TEST_TENANT = new Tenant("test", "Test", "Test tenant", null, false);

  @Test
  void buildSchedulesBothFieldsWhenTablesExist() throws Exception {
    UniqueLabelRedaction anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(
      TEST_TENANT,
      List.of(
        new ModuleTable("circulation_storage", "cancellation_reason", 10),
        new ModuleTable("oa", "checklist_item_definition", 10)
      )
    );

    Job job = anonymization.getBuilders(tenant).getFirst().build();
    List<? extends BatchGenerationFromTablePart<?>> parts = getParts(job, "prepare");

    assertEquals(2, parts.size());
    assertHasLabel(parts, "$.name");
    assertHasLabel(parts, "clid_name");
  }

  @Test
  void buildSkipsWhenTablesMissing() throws Exception {
    UniqueLabelRedaction anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(
      TEST_TENANT,
      List.of(new ModuleTable("users", "outbox_event_log", 10))
    );

    Job job = anonymization.getBuilders(tenant).getFirst().build();
    assertTrue(getParts(job, "prepare").isEmpty());
  }

  private static UniqueLabelRedaction createFactoryWithContext() throws Exception {
    UniqueLabelRedaction anonymization = new UniqueLabelRedaction();
    Field contextField = UniqueLabelRedaction.class.getDeclaredField("context");
    contextField.setAccessible(true);
    contextField.set(anonymization, SharedExecutionContext.forTests());
    return anonymization;
  }

  private static List<? extends BatchGenerationFromTablePart<?>> getParts(Job job, String phase) {
    ConcurrentLinkedQueue<?> partsForPhase = job.getParts().get(phase);
    if (partsForPhase == null) {
      return List.of();
    }
    return partsForPhase.stream().map(part -> (BatchGenerationFromTablePart<?>) part).toList();
  }

  private static void assertHasLabel(List<? extends BatchGenerationFromTablePart<?>> parts, String token) {
    assertTrue(parts.stream().anyMatch(part -> part.getLabel().contains(token)));
  }
}
