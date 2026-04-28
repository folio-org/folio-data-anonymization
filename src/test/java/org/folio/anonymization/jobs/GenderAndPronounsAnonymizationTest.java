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
import org.folio.anonymization.jobs.templates.ReplaceJSONBWithSQLPart;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;

class GenderAndPronounsAnonymizationTest {

  private static final Tenant TEST_TENANT = new Tenant("test", "Test", "Test tenant", null, false);

  @Test
  void buildSchedulesGenderAndPronounsReplacementParts() throws Exception {
    Job job = buildJobWithTables(new ModuleTable("users", "users", 100));
    List<ReplaceJSONBWithSQLPart> parts = getOverwriteParts(job);
    assertEquals(2, parts.size());

    ReplaceJSONBWithSQLPart genderPart = findPart(parts, "$.personal.gender");
    String genderSql = getReplacementSql(genderPart);
    assertTrue(genderSql.contains("Female"));
    assertTrue(genderSql.contains("Male"));
    assertTrue(genderSql.contains("Non-binary"));
    assertTrue(genderSql.contains("Prefer not to say"));

    ReplaceJSONBWithSQLPart pronounsPart = findPart(parts, "$.personal.pronouns");
    String pronounsSql = getReplacementSql(pronounsPart);
    assertTrue(pronounsSql.contains("she/her"));
    assertTrue(pronounsSql.contains("he/him"));
    assertTrue(pronounsSql.contains("they/them"));
    assertTrue(pronounsSql.contains("ze/zir"));
  }

  @Test
  void buildDoesNotSchedulePartsWhenUsersTableIsMissing() throws Exception {
    Job job = buildJobWithTables(new ModuleTable("users", "outbox_event_log", 10));
    assertTrue(getOverwriteParts(job).isEmpty());
  }

  private static Job buildJobWithTables(ModuleTable... tables) throws Exception {
    GenderAndPronounsAnonymization anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(TEST_TENANT, List.of(tables));
    return anonymization.getBuilders(tenant).getFirst().build();
  }

  private static GenderAndPronounsAnonymization createFactoryWithContext() throws Exception {
    GenderAndPronounsAnonymization anonymization = new GenderAndPronounsAnonymization();
    Field contextField = GenderAndPronounsAnonymization.class.getDeclaredField("context");
    contextField.setAccessible(true);
    contextField.set(anonymization, new SharedExecutionContext((DSLContext) null, job -> {}, Runnable::run));
    return anonymization;
  }

  private static List<ReplaceJSONBWithSQLPart> getOverwriteParts(Job job) {
    ConcurrentLinkedQueue<?> overwriteParts = job.getParts().get("overwrite");
    assertNotNull(overwriteParts);
    return overwriteParts.stream().map(ReplaceJSONBWithSQLPart.class::cast).toList();
  }

  private static ReplaceJSONBWithSQLPart findPart(List<ReplaceJSONBWithSQLPart> parts, String jsonPath) {
    return parts
      .stream()
      .filter(part -> part.getLabel().contains(jsonPath))
      .findFirst()
      .orElseThrow(() -> new AssertionError("No part found for json path " + jsonPath));
  }

  private static String getReplacementSql(ReplaceJSONBWithSQLPart part) throws Exception {
    Field replacementSqlField = ReplaceJSONBWithSQLPart.class.getDeclaredField("replacementSql");
    replacementSqlField.setAccessible(true);
    return (String) replacementSqlField.get(part);
  }
}
