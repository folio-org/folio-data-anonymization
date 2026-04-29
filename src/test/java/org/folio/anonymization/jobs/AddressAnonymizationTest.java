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
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

class AddressAnonymizationTest {

  private static final Tenant TEST_TENANT = new Tenant("test", "Test", "Test tenant", null, false);

  @Test
  void buildSchedulesAddressReplacementParts() throws Exception {
    Job job = buildJobWithTables(new ModuleTable("users", "users", 100));
    List<ReplaceJSONBValuePart> parts = getOverwriteParts(job);
    assertEquals(6, parts.size());

    assertRandomArrayJsonbSql(renderedSql(findPart(parts, "$.personal.addresses[*].addressLine1")));
    assertRandomArrayJsonbSql(renderedSql(findPart(parts, "$.personal.addresses[*].addressLine2")));
    assertRandomArrayJsonbSql(renderedSql(findPart(parts, "$.personal.addresses[*].city")));
    assertRandomArrayJsonbSql(renderedSql(findPart(parts, "$.personal.addresses[*].region")));
    assertRandomArrayJsonbSql(renderedSql(findPart(parts, "$.personal.addresses[*].postalCode")));
    assertRandomArrayJsonbSql(renderedSql(findPart(parts, "$.personal.addresses[*].countryId")));
  }

  @Test
  void buildDoesNotSchedulePartsWhenUsersTableIsMissing() throws Exception {
    Job job = buildJobWithTables(new ModuleTable("users", "outbox_event_log", 10));
    assertTrue(getOverwriteParts(job).isEmpty());
  }

  private static Job buildJobWithTables(ModuleTable... tables) throws Exception {
    AddressAnonymization anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(TEST_TENANT, List.of(tables));
    return anonymization.getBuilders(tenant).getFirst().build();
  }

  private static AddressAnonymization createFactoryWithContext() throws Exception {
    AddressAnonymization anonymization = new AddressAnonymization();
    Field contextField = AddressAnonymization.class.getDeclaredField("context");
    contextField.setAccessible(true);
    contextField.set(anonymization, new SharedExecutionContext((DSLContext) null, job -> {}, Runnable::run));
    return anonymization;
  }

  private static List<ReplaceJSONBValuePart> getOverwriteParts(Job job) {
    List<ReplaceJSONBValuePart> parts = job
      .getParts()
      .values()
      .stream()
      .flatMap(ConcurrentLinkedQueue::stream)
      .map(ReplaceJSONBValuePart.class::cast)
      .toList();
    assertNotNull(parts);
    return parts;
  }

  private static ReplaceJSONBValuePart findPart(List<ReplaceJSONBValuePart> parts, String jsonPath) {
    String fullPathToken = "->'" + jsonPath + "'";
    return parts
      .stream()
      .filter(part -> part.getLabel().contains(fullPathToken))
      .findFirst()
      .orElseThrow(() -> new AssertionError("No part found for json path " + jsonPath));
  }

  private static String renderedSql(ReplaceJSONBValuePart part) {
    return DSL.using(SQLDialect.POSTGRES).renderInlined(part.getReplacement().apply(null));
  }

  private static void assertRandomArrayJsonbSql(String sql) {
    assertTrue(sql.contains("to_jsonb"));
    assertTrue(sql.contains("::text[]"));
    assertTrue(sql.contains("floor(random() *"));
  }
}
