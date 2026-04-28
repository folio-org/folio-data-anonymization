package org.folio.anonymization.jobs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import org.folio.anonymization.domain.db.ModuleTable;
import org.folio.anonymization.domain.folio.Tenant;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.ReplaceJSONBWithSQLPart;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockResult;
import org.junit.jupiter.api.Test;

class IPAddressAnonymizationTest {

  private static final Tenant TEST_TENANT = new Tenant("test", "Test", "Test tenant", null, false);
  private static final ModuleTable IP_EVENT_LOGS_TABLE = new ModuleTable("login", "event_logs", 12);
  private static final ModuleTable UNRELATED_TABLE = new ModuleTable("users", "users", 4);

  @Test
  void buildSchedulesIpReplacementPartWhenEventLogsTableExists() throws Exception {
    IPAddressAnonymization anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = tenantWithIpLogsTable();

    List<JobBuilder> builders = anonymization.getBuilders(tenant);
    assertEquals(1, builders.size());

    Job job = builders.getFirst().build();
    assertEquals(List.of("overwrite"), job.getStages());

    ConcurrentLinkedQueue<?> overwriteParts = job.getParts().get("overwrite");
    assertNotNull(overwriteParts);
    assertEquals(1, overwriteParts.size());

    ReplaceJSONBWithSQLPart part = assertInstanceOf(ReplaceJSONBWithSQLPart.class, overwriteParts.peek());
    assertEquals("replace IP (login.event_logs.jsonb->'$.ip')", part.getLabel());
    assertEquals(
      "concat('\"169.254.', trunc(random() * 256), '.', trunc(random() * 256), '\"')::jsonb",
      getReplacementSql(part)
    );
  }

  @Test
  void buildDoesNotSchedulePartWhenEventLogsTableIsMissing() throws Exception {
    IPAddressAnonymization anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = tenantWithoutIpLogsTable();

    JobBuilder builder = anonymization.getBuilders(tenant).getFirst();
    assertTrue(builder.configuration().getFirst().isDisabled());
    assertFalse(builder.configuration().getFirst().isOn());

    Job job = builder.build();
    ConcurrentLinkedQueue<?> overwriteParts = job.getParts().get("overwrite");
    assertNotNull(overwriteParts);
    assertTrue(overwriteParts.isEmpty());
  }

  @Test
  void replacementQuerySetsJsonIpToReservedLinkLocalRange() throws Exception {
    AtomicReference<String> capturedSql = new AtomicReference<>();
    DSLContext mockCreate = DSL.using(
      new MockConnection(ctx -> {
        capturedSql.set(ctx.sql());
        return new MockResult[] { new MockResult(1) };
      }),
      SQLDialect.POSTGRES
    );

    IPAddressAnonymization anonymization = createFactoryWithContext(mockCreate);
    Job job = anonymization
      .getBuilders(tenantWithIpLogsTable())
      .getFirst()
      .build();

    ReplaceJSONBWithSQLPart part = assertInstanceOf(
      ReplaceJSONBWithSQLPart.class,
      job.getParts().get("overwrite").peek()
    );
    part.setJob(job);
    part.setStage("overwrite");
    part.get();

    String sql = capturedSql.get();
    assertNotNull(sql);
    assertTrue(sql.contains("jsonb_set"));
    assertTrue(sql.contains("169.254."));
    assertTrue(sql.contains("trunc(random() * 256)"));
    assertTrue(sql.contains("update"));
  }

  private static IPAddressAnonymization createFactoryWithContext() throws Exception {
    return createFactoryWithContext(null);
  }

  private static IPAddressAnonymization createFactoryWithContext(DSLContext create) throws Exception {
    IPAddressAnonymization anonymization = new IPAddressAnonymization();
    Field contextField = IPAddressAnonymization.class.getDeclaredField("context");
    contextField.setAccessible(true);
    contextField.set(
      anonymization,
      new SharedExecutionContext(create, job -> {}, Runnable::run)
    );
    return anonymization;
  }

  private static String getReplacementSql(ReplaceJSONBWithSQLPart part) throws Exception {
    Field replacementSqlField = ReplaceJSONBWithSQLPart.class.getDeclaredField("replacementSql");
    replacementSqlField.setAccessible(true);
    return (String) replacementSqlField.get(part);
  }

  private static TenantExecutionContext tenantWithIpLogsTable() {
    return new TenantExecutionContext(TEST_TENANT, List.of(IP_EVENT_LOGS_TABLE));
  }

  private static TenantExecutionContext tenantWithoutIpLogsTable() {
    return new TenantExecutionContext(TEST_TENANT, List.of(UNRELATED_TABLE));
  }
}
