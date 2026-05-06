package org.folio.anonymization.jobs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.folio.anonymization.domain.db.ModuleTable;
import org.folio.anonymization.domain.folio.Tenant;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobPart;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.ReplaceJSONBValuePart;
import org.folio.anonymization.jobs.templates.ReplaceValuePart;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;

class UserCredentialsAnonymizationTest {

  private static final Tenant TEST_TENANT = new Tenant("test", "Test", "Test tenant", null, false);

  @Test
  void buildSchedulesAllCredentialPartsWhenTablesExist() throws Exception {
    UserCredentialsAnonymization anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(
      TEST_TENANT,
      List.of(
        new ModuleTable("login", "auth_credentials", 10),
        new ModuleTable("login", "auth_credentials_history", 10),
        new ModuleTable("users", "patronpin", 10)
      )
    );

    Job job = anonymization.getBuilders(tenant).getFirst().build();
    ConcurrentLinkedQueue<?> overwriteParts = job.getParts().get("overwrite");

    assertEquals(5, overwriteParts.size());
    assertEquals(4, overwriteParts.stream().filter(ReplaceValuePart.class::isInstance).count());
    assertEquals(1, overwriteParts.stream().filter(ReplaceJSONBValuePart.class::isInstance).count());
    assertTrue(overwriteParts.stream().anyMatch(part -> ((JobPart) part).getLabel().contains("$.pin")));
  }

  @Test
  void buildSkipsCredentialPartsWhenTablesMissing() throws Exception {
    UserCredentialsAnonymization anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(
      TEST_TENANT,
      List.of(new ModuleTable("users", "outbox_event_log", 10))
    );

    Job job = anonymization.getBuilders(tenant).getFirst().build();
    assertTrue(job.getParts().get("overwrite").isEmpty());
  }

  private static UserCredentialsAnonymization createFactoryWithContext() throws Exception {
    UserCredentialsAnonymization anonymization = new UserCredentialsAnonymization();
    Field contextField = UserCredentialsAnonymization.class.getDeclaredField("context");
    contextField.setAccessible(true);
    contextField.set(anonymization, new SharedExecutionContext((DSLContext) null, Runnable::run));
    return anonymization;
  }
}
