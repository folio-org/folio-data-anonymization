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
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;

class OrganizationEdiFtpCredentialRedactionTest {

  private static final Tenant TEST_TENANT = new Tenant("test", "Test", "Test tenant", null, false);

  @Test
  void buildSchedulesAllCredentialFieldsWhenTableExists() throws Exception {
    OrganizationEdiFtpCredentialRedaction anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(
      TEST_TENANT,
      List.of(new ModuleTable("organizations_storage", "organizations", 10))
    );

    Job job = anonymization.getBuilders(tenant).getFirst().build();
    List<? extends BatchGenerationFromTablePart<?>> parts = getParts(job, "prepare");
    assertEquals(3, parts.size());
    assertHasLabel(parts, "$.edi.ediFtp.serverAddress");
    assertHasLabel(parts, "$.edi.ediFtp.username");
    assertHasLabel(parts, "$.edi.ediFtp.password");
  }

  @Test
  void buildSkipsWhenTableMissing() throws Exception {
    OrganizationEdiFtpCredentialRedaction anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(
      TEST_TENANT,
      List.of(new ModuleTable("users", "outbox_event_log", 10))
    );

    Job job = anonymization.getBuilders(tenant).getFirst().build();
    assertTrue(getParts(job, "prepare").isEmpty());
  }

  private static OrganizationEdiFtpCredentialRedaction createFactoryWithContext() throws Exception {
    OrganizationEdiFtpCredentialRedaction anonymization = new OrganizationEdiFtpCredentialRedaction();
    Field contextField = OrganizationEdiFtpCredentialRedaction.class.getDeclaredField("context");
    contextField.setAccessible(true);
    contextField.set(anonymization, new SharedExecutionContext((DSLContext) null, Runnable::run));
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
