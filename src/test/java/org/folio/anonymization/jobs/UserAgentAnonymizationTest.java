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
import org.folio.anonymization.jobs.templates.ReplaceJSONBValuePart;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;

class UserAgentAnonymizationTest {

  private static final Tenant TEST_TENANT = new Tenant("test", "Test", "Test tenant", null, false);

  @Test
  @SuppressWarnings("unchecked")
  void buildSchedulesUserAgentReplacementFromFakerPool() throws Exception {
    Job job = buildJobWithTables(new ModuleTable("login", "event_logs", 100));
    List<ReplaceJSONBValuePart> parts = getOverwriteParts(job);
    assertEquals(1, parts.size());

    ReplaceJSONBValuePart userAgentPart = parts.getFirst();
    assertTrue(userAgentPart.getLabel().contains("$.userAgent"));

    Field valuesField = UserAgentAnonymization.class.getDeclaredField("USER_AGENT_VALUES");
    valuesField.setAccessible(true);
    List<String> generatedValues = (List<String>) valuesField.get(null);
    assertTrue(generatedValues.size() >= 10 && generatedValues.size() <= 20);
    assertTrue(generatedValues.stream().allMatch(value -> value != null && !value.isBlank()));

    String replacementSql = userAgentPart.getReplacement().toString();
    assertTrue(replacementSql.contains("to_jsonb((ARRAY["));
    assertTrue(replacementSql.contains("floor(random() * 15)::int"));
    assertTrue(replacementSql.contains(generatedValues.getFirst().replace("'", "''")));
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
    contextField.set(anonymization, new SharedExecutionContext((DSLContext) null, job -> {}, Runnable::run));
    return anonymization;
  }

  private static List<ReplaceJSONBValuePart> getOverwriteParts(Job job) {
    ConcurrentLinkedQueue<?> overwriteParts = job.getParts().get("overwrite");
    assertNotNull(overwriteParts);
    return overwriteParts.stream().map(ReplaceJSONBValuePart.class::cast).toList();
  }
}
