package org.folio.anonymization.jobs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.List;
import org.folio.anonymization.domain.db.ModuleTable;
import org.folio.anonymization.domain.folio.Tenant;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobPart;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.ReplaceJSONBValuePart;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;

class PatronPinAnonymizationTest {

  private static final Tenant TEST_TENANT = new Tenant("test", "Test", "Test tenant", null, false);

  @Test
  void buildSchedulesPatronPinPartWhenTableExists() throws Exception {
    PatronPinAnonymization anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(TEST_TENANT, List.of(new ModuleTable("users", "patronpin", 10)));

    Job job = anonymization.getBuilders(tenant).getFirst().build();
    List<?> overwriteParts = job.getParts().get("overwrite").stream().toList();

    assertEquals(1, overwriteParts.size());
    assertTrue(overwriteParts.getFirst() instanceof ReplaceJSONBValuePart);
    assertTrue(((JobPart) overwriteParts.getFirst()).getLabel().contains("$.pin"));
  }

  @Test
  void buildSkipsPatronPinPartWhenTableMissing() throws Exception {
    PatronPinAnonymization anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(TEST_TENANT, List.of(new ModuleTable("users", "outbox_event_log", 10)));

    Job job = anonymization.getBuilders(tenant).getFirst().build();
    assertTrue(job.getParts().get("overwrite").isEmpty());
  }

  private static PatronPinAnonymization createFactoryWithContext() throws Exception {
    PatronPinAnonymization anonymization = new PatronPinAnonymization();
    Field contextField = PatronPinAnonymization.class.getDeclaredField("context");
    contextField.setAccessible(true);
    contextField.set(anonymization, new SharedExecutionContext((DSLContext) null, Runnable::run));
    return anonymization;
  }
}
