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

class ImportProfileRelationshipsAnonymizationTest {

  private static final Tenant TEST_TENANT = new Tenant("test", "Test", "Test tenant", null, false);

  @Test
  void buildSchedulesAllTargetFieldsWhenTablesExist() throws Exception {
    ImportProfileRelationshipsAnonymization anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(
      TEST_TENANT,
      List.of(
        new ModuleTable("di_converter_storage", "action_profiles", 10),
        new ModuleTable("di_converter_storage", "job_profiles", 10),
        new ModuleTable("di_converter_storage", "mapping_profiles", 10),
        new ModuleTable("di_converter_storage", "match_profiles", 10),
        new ModuleTable("di_converter_storage", "profile_snapshots", 10)
      )
    );

    Job job = anonymization.getBuilders(tenant).getFirst().build();
    ConcurrentLinkedQueue<?> prepareParts = job.getParts().get("prepare");
    assertNotNull(prepareParts);
    assertEquals(11, prepareParts.size());

    List<? extends BatchGenerationFromTablePart<?>> parts = prepareParts
      .stream()
      .map(BatchGenerationFromTablePart.class::cast)
      .toList();
    assertTrue(parts.stream().anyMatch(part -> part.getLabel().contains("$.parentProfiles")));
    assertTrue(parts.stream().anyMatch(part -> part.getLabel().contains("$.childProfiles")));
    assertTrue(parts.stream().anyMatch(part -> part.getLabel().contains("$.childSnapshotWrappers")));
  }

  @Test
  void buildSkipsAllFieldsWhenTablesMissing() throws Exception {
    ImportProfileRelationshipsAnonymization anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(
      TEST_TENANT,
      List.of(new ModuleTable("users", "outbox_event_log", 10))
    );

    Job job = anonymization.getBuilders(tenant).getFirst().build();
    assertTrue(job.getParts().getOrDefault("prepare", new ConcurrentLinkedQueue<>()).isEmpty());
  }

  private static ImportProfileRelationshipsAnonymization createFactoryWithContext() throws Exception {
    ImportProfileRelationshipsAnonymization anonymization = new ImportProfileRelationshipsAnonymization();
    Field contextField = ImportProfileRelationshipsAnonymization.class.getDeclaredField("context");
    contextField.setAccessible(true);
    contextField.set(anonymization, new SharedExecutionContext((DSLContext) null, Runnable::run));
    return anonymization;
  }
}
