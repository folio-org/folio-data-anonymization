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
import org.folio.anonymization.domain.job.JobPart;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.BatchGenerationFromTablePart;
import org.folio.anonymization.jobs.templates.ReplaceJSONBValuePart;
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
        new ModuleTable("login", "auth_credentials_history", 10)
      )
    );

    Job job = anonymization.getBuilders(tenant).getFirst().build();
    ConcurrentLinkedQueue<?> prepareParts = job.getParts().get("prepare");
    assertNotNull(prepareParts);
    assertEquals(4, prepareParts.size());

    List<JobPart> generatedOverwriteParts = prepareParts
      .stream()
      .map(BatchGenerationFromTablePart.class::cast)
      .map(BatchGenerationFromTablePart::getFactory)
      .map(factory -> factory.build("", null, 0, 1))
      .toList();

    assertEquals(4, generatedOverwriteParts.size());
    assertEquals(4, generatedOverwriteParts.stream().filter(ReplaceJSONBValuePart.class::isInstance).count());
    assertTrue(generatedOverwriteParts.stream().anyMatch(part -> part.getLabel().contains("$.hash")));
    assertTrue(generatedOverwriteParts.stream().anyMatch(part -> part.getLabel().contains("$.salt")));
  }

  @Test
  void buildSkipsCredentialPartsWhenTablesMissing() throws Exception {
    UserCredentialsAnonymization anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(
      TEST_TENANT,
      List.of(new ModuleTable("users", "outbox_event_log", 10))
    );

    Job job = anonymization.getBuilders(tenant).getFirst().build();
    assertTrue(job.getParts().getOrDefault("prepare", new ConcurrentLinkedQueue<>()).isEmpty());
  }

  private static UserCredentialsAnonymization createFactoryWithContext() throws Exception {
    UserCredentialsAnonymization anonymization = new UserCredentialsAnonymization();
    Field contextField = UserCredentialsAnonymization.class.getDeclaredField("context");
    contextField.setAccessible(true);
    contextField.set(anonymization, new SharedExecutionContext((DSLContext) null, (DSLContext) null, Runnable::run));
    return anonymization;
  }
}
