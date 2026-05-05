package org.folio.anonymization.jobs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.List;
import org.folio.anonymization.domain.db.ModuleTable;
import org.folio.anonymization.domain.folio.Tenant;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.DefaultResourceLoader;

class ProfilePictureAnonymizationTest {

  private static final Tenant TEST_TENANT = new Tenant("test", "Test", "Test tenant", null, false);

  @Test
  void buildCreatesSingleJobAndSchedulesAllStagesWhenTablesExist() throws Exception {
    ProfilePictureAnonymization anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(
      TEST_TENANT,
      List.of(
        new ModuleTable("users", "profile_picture", 5),
        new ModuleTable("users", "configuration", 1),
        new ModuleTable("users", "settings", 1)
      )
    );

    List<JobBuilder> builders = anonymization.getBuilders(tenant);
    assertEquals(1, builders.size());

    Job job = builders.getFirst().build();
    assertTrue(job.getParts().containsKey("update-configuration"));
    assertEquals(3, job.getParts().get("update-configuration").size());
    assertTrue(job.getParts().containsKey("update-settings"));
    assertEquals(3, job.getParts().get("update-settings").size());
    assertTrue(job.getParts().containsKey("replace-pictures"));
    assertEquals(1, job.getParts().get("replace-pictures").size());
  }

  @Test
  void buildSkipsAllStagesWhenTablesMissing() throws Exception {
    ProfilePictureAnonymization anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(
      TEST_TENANT,
      List.of(new ModuleTable("users", "outbox_event_log", 10))
    );

    Job job = anonymization.getBuilders(tenant).getFirst().build();
    assertTrue(
      job.getParts().getOrDefault("update-configuration", new java.util.concurrent.ConcurrentLinkedQueue<>()).isEmpty()
    );
    assertTrue(job.getParts().getOrDefault("update-settings", new java.util.concurrent.ConcurrentLinkedQueue<>()).isEmpty());
    assertTrue(job.getParts().getOrDefault("replace-pictures", new java.util.concurrent.ConcurrentLinkedQueue<>()).isEmpty());
  }

  private static ProfilePictureAnonymization createFactoryWithContext() throws Exception {
    ProfilePictureAnonymization anonymization = new ProfilePictureAnonymization();
    Field contextField = ProfilePictureAnonymization.class.getDeclaredField("context");
    contextField.setAccessible(true);
    contextField.set(anonymization, new SharedExecutionContext((DSLContext) null, job -> {}, Runnable::run));
    Field resourceLoaderField = ProfilePictureAnonymization.class.getDeclaredField("resourceLoader");
    resourceLoaderField.setAccessible(true);
    resourceLoaderField.set(anonymization, new DefaultResourceLoader());
    return anonymization;
  }
}
