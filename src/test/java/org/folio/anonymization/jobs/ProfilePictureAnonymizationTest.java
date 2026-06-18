package org.folio.anonymization.jobs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.folio.anonymization.domain.db.ModuleTable;
import org.folio.anonymization.domain.folio.Tenant;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.service.ProfilePictureSeedCsvLoader;
import org.folio.anonymization.service.SeedFileService;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;

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
    assertTrue(job.getParts().containsKey("prepare-replace-pictures"));
    assertEquals(1, job.getParts().get("prepare-replace-pictures").size());
    assertTrue(job.getParts().getOrDefault("replace-pictures", new ConcurrentLinkedQueue<>()).isEmpty());
  }

  @Test
  void buildSkipsAllStagesWhenRequiredTablesMissing() throws Exception {
    ProfilePictureAnonymization anonymization = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(
      TEST_TENANT,
      List.of(new ModuleTable("users", "outbox_event_log", 10))
    );

    Job job = anonymization.getBuilders(tenant).getFirst().build();
    assertTrue(job.getParts().getOrDefault("update-configuration", new ConcurrentLinkedQueue<>()).isEmpty());
    assertTrue(job.getParts().getOrDefault("update-settings", new ConcurrentLinkedQueue<>()).isEmpty());
    assertTrue(job.getParts().getOrDefault("prepare-replace-pictures", new ConcurrentLinkedQueue<>()).isEmpty());
    assertTrue(job.getParts().getOrDefault("replace-pictures", new ConcurrentLinkedQueue<>()).isEmpty());
  }

  private static ProfilePictureAnonymization createFactoryWithContext() throws Exception {
    ProfilePictureAnonymization anonymization = new ProfilePictureAnonymization(
      new SharedExecutionContext((DSLContext) null, (DSLContext) null, Runnable::run),
      new ProfilePictureSeedCsvLoader(new MockSeedFileService())
    );
    return anonymization;
  }

  private static class MockSeedFileService extends SeedFileService {

    public MockSeedFileService() {
      super();
    }

    @Override
    public InputStream getSeedFileAsInputStream(String filename) {
      return new ByteArrayInputStream(
        (
          "id,profile_picture_blob,hmac\n" +
          "something,YzI5dFpYUm9hVzVu,c29tZXRoaW5n\n" +
          "something,YzI5dFpYUm9hVzVu,c29tZXRoaW5n\n" +
          "something,YzI5dFpYUm9hVzVu,c29tZXRoaW5n\n"
        ).getBytes()
      );
    }
  }
}
