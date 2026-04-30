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
import org.folio.anonymization.jobs.templates.ReplaceJSONBValuePart;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;

class DateOfBirthAnonymizationTest {

  private static final Tenant TEST_TENANT = new Tenant("test", "Test", "Test tenant", null, false);

  @Test
  void buildSchedulesDobReplacementPart() throws Exception {
    Job job = buildJobWithTables(new ModuleTable("users", "users", 100));
    List<ReplaceJSONBValuePart> parts = getOverwriteParts(job);
    assertEquals(1, parts.size());

    ReplaceJSONBValuePart dobPart = parts.getFirst();
    assertTrue(dobPart.getLabel().contains("$.personal.dateOfBirth"));

    String dobSql = dobPart.getReplacement().apply(null).toString();
    assertTrue(dobSql.contains("1940-01-01"));
    assertTrue(dobSql.contains("2007-12-31"));
    assertTrue(dobSql.contains("YYYY-MM-DD"));
    assertTrue(dobSql.contains("to_jsonb"));
  }

  @Test
  void buildDoesNotSchedulePartsWhenUsersTableIsMissing() throws Exception {
    Job job = buildJobWithTables(new ModuleTable("users", "outbox_event_log", 10));
    assertTrue(getOverwriteParts(job).isEmpty());
  }

  private static Job buildJobWithTables(ModuleTable... tables) throws Exception {
    DateOfBirthAnonymization anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(TEST_TENANT, List.of(tables));
    return anonymization.getBuilders(tenant).getFirst().build();
  }

  private static DateOfBirthAnonymization createFactoryWithContext() throws Exception {
    DateOfBirthAnonymization anonymization = new DateOfBirthAnonymization();
    Field contextField = DateOfBirthAnonymization.class.getDeclaredField("context");
    contextField.setAccessible(true);
    contextField.set(anonymization, new SharedExecutionContext((DSLContext) null, job -> {}, Runnable::run));
    return anonymization;
  }

  private static List<ReplaceJSONBValuePart> getOverwriteParts(Job job) {
    ConcurrentLinkedQueue<?> prepareParts = job.getParts().get("prepare");
    assertNotNull(prepareParts);
    return prepareParts
      .stream()
      .map(BatchGenerationFromTablePart.class::cast)
      .map(BatchGenerationFromTablePart::getFactory)
      .map(f -> f.build("", null, 0, 1))
      .map(ReplaceJSONBValuePart.class::cast)
      .toList();
  }
}
