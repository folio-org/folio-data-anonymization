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
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.BatchGenerationFromTablePart;
import org.junit.jupiter.api.Test;

class FinancialInformationAnonymizationTest {

  private static final Tenant TEST_TENANT = new Tenant("test", "Test", "Test tenant", null, false);

  @Test
  void buildSchedulesBankTransactionRedactionParts() throws Exception {
    Job job = buildJobWithTables(
      0,
      new ModuleTable("feesfines", "feefineactions", 100),
      new ModuleTable("organizations_storage", "banking_information", 100)
    );
    List<? extends BatchGenerationFromTablePart<?>> parts = getParts(job, "prepare");
    assertEquals(2, parts.size());
    assertHasLabel(parts, "$.transactionInformation");
    assertHasLabel(parts, "$.bankName");
  }

  @Test
  void buildSchedulesAccountNumberAnonymizationParts() throws Exception {
    Job job = buildJobWithTables(
      1,
      new ModuleTable("organizations_storage", "banking_information", 100),
      new ModuleTable("organizations_storage", "organizations", 100),
      new ModuleTable("orders_storage", "po_line", 100)
    );
    List<? extends BatchGenerationFromTablePart<?>> parts = getParts(job, "enumerate-prep");
    assertEquals(3, parts.size());
    assertHasLabel(parts, "$.bankAccountNumber");
    assertHasLabel(parts, "$.accounts[*].accountNo");
    assertHasLabel(parts, "$.vendorDetail.vendorAccount");
  }

  @Test
  void buildSchedulesRoutingNumberReplacementParts() throws Exception {
    Job job = buildJobWithTables(2, new ModuleTable("organizations_storage", "banking_information", 100));
    List<? extends BatchGenerationFromTablePart<?>> parts = getParts(job, "prepare");
    assertEquals(1, parts.size());
    assertHasLabel(parts, "$.transitNumber");
  }

  @Test
  void buildDoesNotSchedulePartsWhenTargetTablesAreMissing() throws Exception {
    Job redactionJob = buildJobWithTables(0, new ModuleTable("users", "outbox_event_log", 10));
    Job accountJob = buildJobWithTables(1, new ModuleTable("users", "outbox_event_log", 10));
    Job routingJob = buildJobWithTables(2, new ModuleTable("users", "outbox_event_log", 10));

    assertTrue(getParts(redactionJob, "prepare").isEmpty());
    assertTrue(getParts(accountJob, "enumerate-prep").isEmpty());
    assertTrue(getParts(routingJob, "prepare").isEmpty());
  }

  private static Job buildJobWithTables(int builderIndex, ModuleTable... tables) throws Exception {
    FinancialInformationAnonymization anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(TEST_TENANT, List.of(tables));
    List<JobBuilder> builders = anonymization.getBuilders(tenant);
    Job job = builders.get(builderIndex).build();
    List<JobConfigurationProperty> settings = job.getContext().settings();
    settings.stream().filter(s -> "drop-table".equals(s.getKey())).forEach(s -> s.setBooleanValue(false));
    return job;
  }

  private static FinancialInformationAnonymization createFactoryWithContext() throws Exception {
    FinancialInformationAnonymization anonymization = new FinancialInformationAnonymization();
    Field contextField = FinancialInformationAnonymization.class.getDeclaredField("context");
    contextField.setAccessible(true);
    contextField.set(anonymization, SharedExecutionContext.forTests());
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
