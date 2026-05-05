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

class ProfilePictureAnonymizationTest {

  private static final Tenant TEST_TENANT = new Tenant("test", "Test", "Test tenant", null, false);

  @Test
  void buildCreatesSingleJobAndSchedulesConfigAndReplacementWhenTableExists() throws Exception {
    ProfilePictureAnonymization anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(TEST_TENANT, List.of(new ModuleTable(
      "users",
      "profile_picture",
      5
    )));

    List<JobBuilder> builders = anonymization.getBuilders(tenant);
    assertEquals(1, builders.size());

    Job job = builders.getFirst().build();
    assertTrue(job.getParts().containsKey("update-config"));
    assertEquals(1, job.getParts().get("update-config").size());
    assertTrue(job.getParts().containsKey("replace"));
    assertEquals(1, job.getParts().get("replace").size());
  }

  @Test
  void buildSkipsReplacementWhenTableMissing() throws Exception {
    ProfilePictureAnonymization anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(TEST_TENANT, List.of(new ModuleTable(
      "users",
      "outbox_event_log",
      10
    )));

    Job job = anonymization.getBuilders(tenant).getFirst().build();
    assertTrue(job.getParts().containsKey("update-config"));
    assertEquals(1, job.getParts().get("update-config").size());
    assertTrue(job.getParts().getOrDefault("replace", new java.util.concurrent.ConcurrentLinkedQueue<>()).isEmpty());
  }

  private static ProfilePictureAnonymization createFactoryWithContext() throws Exception {
    ProfilePictureAnonymization anonymization = new ProfilePictureAnonymization();
    Field contextField = ProfilePictureAnonymization.class.getDeclaredField("context");
    contextField.setAccessible(true);
    contextField.set(anonymization, new SharedExecutionContext((DSLContext) null, job -> {}, Runnable::run));

    Field encryptionKeyField = ProfilePictureAnonymization.class.getDeclaredField("encryptionKey");
    encryptionKeyField.setAccessible(true);
    encryptionKeyField.set(anonymization, "anonymizedanonymizedanonymizedan");

    Field seedCsvPathField = ProfilePictureAnonymization.class.getDeclaredField("seedCsvPath");
    seedCsvPathField.setAccessible(true);
    seedCsvPathField.set(anonymization, "classpath:/seed/profile-picture-seed.csv");
    return anonymization;
  }
}

