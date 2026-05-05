package org.folio.anonymization.jobs;

import static org.jooq.impl.DSL.condition;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.inline;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import org.folio.anonymization.config.JobConfig;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.BatchGenerationFromTablePart;
import org.folio.anonymization.jobs.templates.ReplaceJSONBValuePart;
import org.folio.anonymization.jobs.templates.ReplaceValueFromListPart;
import org.folio.anonymization.util.ProfilePictureSeedCsvLoader;
import org.folio.anonymization.util.ProfilePictureSeedCsvLoader.SeedValue;
import org.jooq.Condition;
import org.jooq.JSONB;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

@Component
public class ProfilePictureAnonymization implements JobFactory {

  private static final String PROFILE_PICTURE_ENCRYPTION_KEY = "anonymizedanonymizedanonymizedan";
  private static final String PROFILE_PICTURE_SEED_LOCATION = "classpath:seed/profile-picture-seed.csv";

  @Autowired
  private SharedExecutionContext context;

  @Autowired
  private ResourceLoader resourceLoader;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    boolean hasProfilePictureTable = hasTable(tenant, "users", "profile_picture");
    boolean hasConfigurationTable = hasTable(tenant, "users", "configuration");
    boolean hasSettingsTable = hasTable(tenant, "users", "settings");

    List<JobConfigurationProperty> configuration = List.of(
      new JobConfigurationProperty(
        "update-configuration",
        "Replace profile picture encryption settings in mod_users.configuration",
        true,
        !hasConfigurationTable
      ),
      new JobConfigurationProperty(
        "update-settings",
        "Replace profile picture encryption settings in mod_users.settings",
        true,
        !hasSettingsTable
      ),
      new JobConfigurationProperty(
        "replace-pictures",
        "Replace mod_users.profile_picture blob and hmac values from seed data",
        true,
        !hasProfilePictureTable
      )
    );

    return List.of(
      new JobBuilder(
        "Profile picture anonymization",
        "Replaces profile picture encryption settings and swaps users.profile_picture values with seeded data.",
        tenant,
        context,
        configuration,
        ctx -> {
          Job job = new Job(ctx, List.of("update-configuration", "update-settings", "prepare-replace-pictures", "replace-pictures"));
          if (JobConfigurationProperty.isOn(ctx.settings(), "update-configuration")) {
            job.scheduleParts("update-configuration", buildConfigUpdateParts(tenant, "configuration"));
          }
          if (JobConfigurationProperty.isOn(ctx.settings(), "update-settings")) {
            job.scheduleParts("update-settings", buildConfigUpdateParts(tenant, "settings"));
          }
          if (JobConfigurationProperty.isOn(ctx.settings(), "replace-pictures")) {
            List<SeedValue> seeds = ProfilePictureSeedCsvLoader.load(resourceLoader.getResource(PROFILE_PICTURE_SEED_LOCATION));
            int runOffset = seeds.size() > 1 ? ThreadLocalRandom.current().nextInt(seeds.size()) : 0;

            job.scheduleParts(
              "prepare-replace-pictures",
              List.of(
                new BatchGenerationFromTablePart<UUID>(
                  "Prepare batches for users.profile_picture replacements",
                  new FieldReference("users", "profile_picture", "id"),
                  UUID.class,
                  JobConfig.BATCH_SIZE,
                  "replace-pictures",
                  (label, condition, start, end) ->
                    new ReplaceValueFromListPart(
                      "Replace users.profile_picture blob+hmac on " + label,
                      List.of(
                        new FieldReference("users", "profile_picture", "profile_picture_blob"),
                        new FieldReference("users", "profile_picture", "hmac")
                      ),
                      condition,
                      List.of(
                        seeds.stream().map(SeedValue::profilePictureBlob).toList(),
                        seeds.stream().map(SeedValue::hmac).toList()
                      ),
                      List.of(field("profile_picture_blob", byte[].class), field("hmac", byte[].class))
                    )
                )
              )
            );
          }
          return job;
        }
      )
    );
  }

  private static boolean hasTable(TenantExecutionContext tenant, String schema, String table) {
    return tenant
      .availableTables()
      .stream()
      .anyMatch(candidate -> schema.equals(candidate.schema()) && table.equals(candidate.table()));
  }

  private static List<ReplaceJSONBValuePart> buildConfigUpdateParts(TenantExecutionContext tenant, String table) {
    FieldReference rowJson = new FieldReference("users", table, "jsonb");
    Condition profilePictureConfigCondition = condition(
      "{0} ->> 'key' = 'PROFILE_PICTURE_CONFIG' and {0} ->> 'scope' = 'mod-users'",
      rowJson.baseColumn(tenant.tenant(), JSONB.class)
    );

    return List.of(
      new ReplaceJSONBValuePart(
        "Set PROFILE_PICTURE_CONFIG encryptionKey in users." + table,
        new FieldReference("users", table, "jsonb", "$.value.encryptionKey"),
        profilePictureConfigCondition,
        field("to_jsonb(cast({0} as text))", JSONB.class, inline(PROFILE_PICTURE_ENCRYPTION_KEY))
      ),
      new ReplaceJSONBValuePart(
        "Set PROFILE_PICTURE_CONFIG enabledObjectStorage=false in users." + table,
        new FieldReference("users", table, "jsonb", "$.value.enabledObjectStorage"),
        profilePictureConfigCondition,
        field("to_jsonb({0})", JSONB.class, inline(false))
      ),
      new ReplaceJSONBValuePart(
        "Set PROFILE_PICTURE_CONFIG enabled=true in users." + table,
        new FieldReference("users", table, "jsonb", "$.value.enabled"),
        profilePictureConfigCondition,
        field("to_jsonb({0})", JSONB.class, inline(true))
      )
    );
  }

  private static List<byte[]> replacementColumn(
    List<SeedValue> seeds,
    int offset,
    int size,
    Function<SeedValue, byte[]> extractor
  ) {
    List<byte[]> values = new ArrayList<>(size);
    int seedCount = seeds.size();
    for (int i = 0; i < size; i++) {
      values.add(extractor.apply(seeds.get((offset + i) % seedCount)));
    }
    return values;
  }
}
