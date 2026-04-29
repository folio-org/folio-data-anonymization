package org.folio.anonymization.jobs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
import org.folio.anonymization.jobs.templates.ReplaceJSONBWithSQLPart;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;

class IPAddressAnonymizationTest {

  private static final Tenant TEST_TENANT = new Tenant("test", "Test", "Test tenant", null, false);

  @Test
  void buildSchedulesIpReplacementPartWhenEventLogsTableExists() throws Exception {
    Job job = buildJobWithTables(new ModuleTable("login", "event_logs", 12));

    ConcurrentLinkedQueue<?> overwriteParts = job.getParts().get("overwrite");
    assertNotNull(overwriteParts);
    assertEquals(1, overwriteParts.size());

    ReplaceJSONBWithSQLPart part = assertInstanceOf(ReplaceJSONBWithSQLPart.class, overwriteParts.peek());
    assertEquals("replace IP (login.event_logs.jsonb->'$.ip')", part.getLabel());
    String replacementSql = getReplacementSql(part);
    assertTrue(replacementSql.contains("169.254."));
    assertTrue(replacementSql.contains("random() * 256"));
    assertTrue(replacementSql.contains("::jsonb"));
  }

  @Test
  void buildDoesNotSchedulePartWhenEventLogsTableIsMissing() throws Exception {
    Job job = buildJobWithTables(new ModuleTable("users", "users", 4));
    ConcurrentLinkedQueue<?> overwriteParts = job.getParts().get("overwrite");
    assertNotNull(overwriteParts);
    assertTrue(overwriteParts.isEmpty());
  }

  private static Job buildJobWithTables(ModuleTable... tables) throws Exception {
    IPAddressAnonymization anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(TEST_TENANT, List.of(tables));
    return anonymization.getBuilders(tenant).getFirst().build();
  }

  private static IPAddressAnonymization createFactoryWithContext() throws Exception {
    IPAddressAnonymization anonymization = new IPAddressAnonymization();
    Field contextField = IPAddressAnonymization.class.getDeclaredField("context");
    contextField.setAccessible(true);
    contextField.set(
      anonymization,
      new SharedExecutionContext((DSLContext) null, job -> {}, Runnable::run)
    );
    return anonymization;
  }

  private static String getReplacementSql(ReplaceJSONBWithSQLPart part) throws Exception {
    Field replacementSqlField = ReplaceJSONBWithSQLPart.class.getDeclaredField("replacementSql");
    replacementSqlField.setAccessible(true);
    return (String) replacementSqlField.get(part);
  }
}
