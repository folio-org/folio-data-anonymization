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

class FinancialInformationAnonymizationTest {

  private static final Tenant TEST_TENANT = new Tenant("test", "Test", "Test tenant", null, false);

  @Test
  void buildSchedulesFinancialInformationReplacementBatchParts() throws Exception {
    Job job = buildJobWithTables(
      new ModuleTable("feesfines", "feefineactions", 100),
      new ModuleTable("organizations_storage", "banking_information", 100),
      new ModuleTable("organizations_storage", "organizations", 100)
    );
    List<? extends BatchGenerationFromTablePart<?>> redactParts = getParts(job, "redact-prep");
    assertEquals(2, redactParts.size());
    assertHasLabel(redactParts, "$.transactionInformation");
    assertHasLabel(redactParts, "$.bankName");

    List<? extends BatchGenerationFromTablePart<?>> replacementParts = getParts(job, "enumerate-prep");
    assertEquals(2, replacementParts.size());
    assertHasLabel(replacementParts, "$.bankAccountNumber");
    assertHasLabel(replacementParts, "$.accounts[*].accountNo");

    List<? extends BatchGenerationFromTablePart<?>> constantParts = getParts(job, "apply-constant-values-prep");
    assertEquals(1, constantParts.size());
    assertHasLabel(constantParts, "$.transitNumber");
  }

  @Test
  void buildDoesNotSchedulePartsWhenTargetTablesAreMissing() throws Exception {
    Job job = buildJobWithTables(new ModuleTable("users", "outbox_event_log", 10));
    assertTrue(getParts(job, "redact-prep").isEmpty());
    assertTrue(getParts(job, "enumerate-prep").isEmpty());
    assertTrue(getParts(job, "apply-constant-values-prep").isEmpty());
  }

  private static Job buildJobWithTables(ModuleTable... tables) throws Exception {
    FinancialInformationAnonymization anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(TEST_TENANT, List.of(tables));
    Job job = anonymization.getBuilders(tenant).getFirst().build();
    List<JobConfigurationProperty> settings = job.getContext().settings();
    settings.stream().filter(s -> "drop-table".equals(s.getKey())).forEach(s -> s.setBooleanValue(false));
    return job;
  }

  private static FinancialInformationAnonymization createFactoryWithContext() throws Exception {
    FinancialInformationAnonymization anonymization = new FinancialInformationAnonymization();
    Field contextField = FinancialInformationAnonymization.class.getDeclaredField("context");
    contextField.setAccessible(true);
    contextField.set(anonymization, new SharedExecutionContext((DSLContext) null, job -> {}, Runnable::run));
    return anonymization;
  }

  private static List<? extends BatchGenerationFromTablePart<?>> getParts(Job job, String phase) {
    ConcurrentLinkedQueue<?> partsForPhase = job.getParts().get(phase);
    if (partsForPhase == null) {
      return List.of();
    }
    List<? extends BatchGenerationFromTablePart<?>> parts = partsForPhase
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
