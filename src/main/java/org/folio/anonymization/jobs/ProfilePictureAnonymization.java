package org.folio.anonymization.jobs;

import java.util.List;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.ForceProfilePictureConfigPart;
import org.folio.anonymization.jobs.templates.ReplaceProfilePicturesFromSeedPart;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ProfilePictureAnonymization implements JobFactory {

  private static final List<FieldReference> PROFILE_PICTURE_FIELDS = List.of(
    new FieldReference("users", "profile_picture", "profile_picture_blob"),
    new FieldReference("users", "profile_picture", "hmac")
  );

  @Value("${anonymization.profile-picture.encryption-key:anonymizedanonymizedanonymizedan}")
  private String encryptionKey;

  @Value("${anonymization.profile-picture.seed-csv-path:classpath:/seed/profile-picture-seed.csv}")
  private String seedCsvPath;

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "Profile picture anonymization",
        "Forces PROFILE_PICTURE_CONFIG and replaces users.profile_picture blob+hmac using seeded values.",
        tenant,
        context,
        JobConfigurationProperty.fromFieldList(PROFILE_PICTURE_FIELDS, tenant),
        ctx -> {
          Job job = new Job(ctx, List.of("update-config", "replace"));
          job.scheduleParts(
            "update-config",
            List.of(new ForceProfilePictureConfigPart("Force PROFILE_PICTURE_CONFIG in settings", encryptionKey))
          );

          if (JobConfigurationProperty.getEnabledFields(ctx.settings()).findAny().isPresent()) {
            job.scheduleParts(
              "replace",
              List.of(
                new ReplaceProfilePicturesFromSeedPart(
                  "Replace users.profile_picture blob+hmac from seed CSV",
                  seedCsvPath
                )
              )
            );
          }
          return job;
        }
      )
    );
  }
}

