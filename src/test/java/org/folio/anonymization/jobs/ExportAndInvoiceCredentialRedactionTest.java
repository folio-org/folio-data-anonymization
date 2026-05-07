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

class ExportAndInvoiceCredentialRedactionTest {

  private static final Tenant TEST_TENANT = new Tenant("test", "Test", "Test tenant", null, false);

  @Test
  void buildSchedulesAllFieldsWhenTablesExist() throws Exception {
    ExportAndInvoiceCredentialRedaction anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(
      TEST_TENANT,
      List.of(
        new ModuleTable("data_export_spring", "export_config", 10),
        new ModuleTable("data_export_spring", "job", 10),
        new ModuleTable("invoice_storage", "batch_voucher_export_configs", 10),
        new ModuleTable("invoice_storage", "export_config_credentials", 10)
      )
    );

    Job job = anonymization.getBuilders(tenant).getFirst().build();
    List<? extends BatchGenerationFromTablePart<?>> parts = getParts(job, "prepare");

    assertEquals(11, parts.size());
    assertHasLabel(parts, "$.vendorEdiOrdersExportConfig.ediFtp.serverAddress");
    assertHasLabel(parts, "$.vendorEdiOrdersExportConfig.ediFtp.username");
    assertHasLabel(parts, "$.vendorEdiOrdersExportConfig.ediFtp.password");
    assertHasLabel(parts, "$.uploadURI");
    assertHasLabel(parts, "$.ftpFormat");
    assertHasLabel(parts, "$.ftpPort");
  }

  @Test
  void buildSkipsWhenTablesMissing() throws Exception {
    ExportAndInvoiceCredentialRedaction anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(
      TEST_TENANT,
      List.of(new ModuleTable("users", "outbox_event_log", 10))
    );

    Job job = anonymization.getBuilders(tenant).getFirst().build();
    assertTrue(getParts(job, "prepare").isEmpty());
  }

  private static ExportAndInvoiceCredentialRedaction createFactoryWithContext() throws Exception {
    ExportAndInvoiceCredentialRedaction anonymization = new ExportAndInvoiceCredentialRedaction();
    Field contextField = ExportAndInvoiceCredentialRedaction.class.getDeclaredField("context");
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
