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
import org.folio.anonymization.jobs.templates.ReplaceValueFromListPart;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;

class UserAgentAnonymizationTest {

  private static final Tenant TEST_TENANT = new Tenant("test", "Test", "Test tenant", null, false);

  @Test
  void buildSchedulesUserAgentReplacementFromFakerPool() throws Exception {
    Job job = buildJobWithTables(new ModuleTable("login", "event_logs", 100));
    List<ReplaceValueFromListPart> parts = getOverwriteParts(job);
    assertEquals(1, parts.size());

    ReplaceValueFromListPart userAgentPart = parts.getFirst();
    assertTrue(userAgentPart.getLabel().contains("$.userAgent"));

    @SuppressWarnings("unchecked")
    List<String> replacements = (List<String>) userAgentPart.getReplacements().get(0);
    assertTrue(replacements.size() >= 10 && replacements.size() <= 20);
    assertTrue(replacements.stream().allMatch(value -> value != null && !value.isBlank()));
  }

  @Test
  void buildDoesNotSchedulePartWhenEventLogsTableIsMissing() throws Exception {
    Job job = buildJobWithTables(new ModuleTable("users", "users", 10));
    assertTrue(getOverwriteParts(job).isEmpty());
  }

  private static Job buildJobWithTables(ModuleTable... tables) throws Exception {
    UserAgentAnonymization anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(TEST_TENANT, List.of(tables));
    return anonymization.getBuilders(tenant).getFirst().build();
  }

  private static UserAgentAnonymization createFactoryWithContext() throws Exception {
    UserAgentAnonymization anonymization = new UserAgentAnonymization();
    Field contextField = UserAgentAnonymization.class.getDeclaredField("context");
    contextField.setAccessible(true);
    contextField.set(anonymization, new SharedExecutionContext((DSLContext) null, Runnable::run));
    return anonymization;
  }

  private static List<ReplaceValueFromListPart> getOverwriteParts(Job job) {
    ConcurrentLinkedQueue<?> prepareParts = job.getParts().get("prepare");
    assertNotNull(prepareParts);
    return prepareParts
      .stream()
      .map(BatchGenerationFromTablePart.class::cast)
      .map(BatchGenerationFromTablePart::getFactory)
      .map(f -> f.build("", null, 0, 15))
      .map(ReplaceValueFromListPart.class::cast)
      .toList();
  }
}
