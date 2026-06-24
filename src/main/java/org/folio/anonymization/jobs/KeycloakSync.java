package org.folio.anonymization.jobs;

import static dev.tamboui.toolkit.Toolkit.row;
import static dev.tamboui.toolkit.Toolkit.spacer;
import static dev.tamboui.toolkit.Toolkit.text;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.primaryKey;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.DSL.translate;
import static org.jooq.impl.DSL.unique;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.folio.anonymization.config.JobConfig;
import org.folio.anonymization.domain.db.GlobalFieldReference;
import org.folio.anonymization.domain.db.TableReference;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.JobPart;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.BatchGenerationFromTablePart;
import org.folio.anonymization.jobs.templates.CreateTablePart;
import org.folio.anonymization.jobs.templates.DropTablePart;
import org.folio.anonymization.repository.UtilRepository;
import org.folio.anonymization.util.RandomValueUtils;
import org.jooq.Field;
import org.jooq.Query;
import org.jooq.Record5;
import org.jooq.Table;
import org.jooq.impl.SQLDataType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Log4j2
@Component
@SuppressWarnings("unchecked")
public class KeycloakSync implements JobFactory {

  @Autowired
  private SharedExecutionContext context;

  @Autowired
  private UtilRepository utilRepository;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    boolean isKeycloakConfigured = utilRepository.doesKeycloakExist();

    return List.of(
      new JobBuilder(
        "keycloak_sync",
        "[Keycloak] Synchronize anonymized data with Keycloak + reset passwords",
        "Updates Keycloak to match the anonymized data in the FOLIO database and sets passwords to 'folio'. Runs after main anonymization.",
        tenant,
        context,
        getProperties(
          isKeycloakConfigured,
          Pair.of(
            "create-table",
            "Create temporary table in KC database with current and new values (disable if resuming a previous run)"
          ),
          Pair.of("drop-table", "Destroy temporary table (PII will be left behind if disabled)"),
          Pair.of("reset-credentials", "Reset user credentials to 'folio' and remove labels, if applicable"),
          Pair.of("reset-idp", "Reset IDP/SSO external IDs to the user's (new) username"),
          Pair.of(
            "update-user-info",
            "Sync user info (username, email, first name, last name) to match the anonymized data"
          )
        ),
        ctx -> {
          Job job = new Job(
            ctx,
            List.of("prepare", "enumerate-prep", "enumerate", "apply-new-values-prep", "apply-new-values", "cleanup")
          );
          job.setDeferred(true);

          Table<?> keycloakUserEntityTable = table(name("public", "user_entity"));
          Field<String> keycloakUserEntityIdField = field("id", SQLDataType.VARCHAR.notNull());

          Table<?> keycloakUserAttributeTable = table(name("public", "user_attribute"));

          Table<?> tempMatchedUserIds = table(
            name("public", "_danon_kc_" + ctx.tenant().tenant().id() + "_folio_user_ids")
          );

          Field<String> matchedKeycloakId = field("kc_id", SQLDataType.VARCHAR.notNull());
          Field<String> matchedFolioId = field("folio_id", SQLDataType.VARCHAR.notNull());
          GlobalFieldReference matchedFolioIdFR = new GlobalFieldReference(
            "public",
            "_danon_kc_" + ctx.tenant().tenant().id() + "_folio_user_ids",
            "folio_id"
          );

          Table<?> tempUserDataTable = table(name("public", "_danon_kc_" + ctx.tenant().tenant().id() + "_user_data"));

          Field<String> dataKeycloakId = field("kc_id", SQLDataType.VARCHAR.notNull());
          Field<String> dataFolioId = field("folio_id", SQLDataType.VARCHAR.notNull());
          Field<String> dataUsername = field("username", SQLDataType.VARCHAR.notNull());
          Field<String> dataEmail = field("email", SQLDataType.VARCHAR.null_());
          Field<String> dataFirstName = field("first_name", SQLDataType.VARCHAR.null_());
          Field<String> dataLastName = field("last_name", SQLDataType.VARCHAR.null_());
          GlobalFieldReference dataKeycloakIdFR = new GlobalFieldReference(
            "public",
            "_danon_kc_" + ctx.tenant().tenant().id() + "_user_data",
            "kc_id"
          );

          Field<String> userId = field("user_id", SQLDataType.VARCHAR.notNull());

          if (JobConfigurationProperty.isOn(ctx.settings(), "create-table")) {
            job.scheduleParts(
              "prepare",
              List.of(
                // CREATE AS is not supported by our CreateTablePart
                new JobPart("Create temporary table (user ID mapping)") {
                  @Override
                  protected void execute() {
                    ctx.executionContext().createKeycloak().dropTableIfExists(tempMatchedUserIds).cascade().execute();

                    this.createKeycloak()
                      .createTable(tempMatchedUserIds)
                      .columns(matchedKeycloakId, matchedFolioId)
                      .as(
                        select(
                          field(name("ue", "id"), String.class).as(matchedKeycloakId),
                          field(name("uaid", "value"), String.class).as(matchedFolioId)
                        )
                          .from(keycloakUserEntityTable.as("ue"))
                          .join(keycloakUserAttributeTable.as("uaid"))
                          .on(
                            field(name("ue", "id"), String.class)
                              .eq(field(name("uaid", "user_id"), String.class))
                              .and(field(name("uaid", "name"), String.class).eq("user_id"))
                          )
                          .join(keycloakUserAttributeTable.as("uat"))
                          .on(
                            field(name("ue", "id"), String.class)
                              .eq(field(name("uat", "user_id"), String.class))
                              .and(field(name("uat", "name"), String.class).eq("tenant_name"))
                              .and(field(name("uat", "value"), String.class).eq(ctx.tenant().tenant().id()))
                          )
                      )
                      .execute();

                    this.createKeycloak().alterTable(tempMatchedUserIds).add(primaryKey(matchedKeycloakId)).execute();
                    this.createKeycloak().alterTable(tempMatchedUserIds).add(unique(matchedFolioId)).execute();
                  }
                },
                new CreateTablePart(
                  "Create temporary table (new user data)",
                  tempUserDataTable,
                  List.of(dataKeycloakId, dataFolioId, dataUsername, dataEmail, dataFirstName, dataLastName),
                  List.of(primaryKey(dataKeycloakId), unique(dataFolioId)),
                  false
                )
                  .withKeycloakDb()
              )
            );

            job.scheduleParts(
              "enumerate-prep",
              List.of(
                new BatchGenerationFromTablePart<>(
                  "Make batches to enumerate data from keycloak user mapping table",
                  matchedFolioIdFR,
                  String.class,
                  JobConfig.BATCH_SIZE,
                  "enumerate",
                  (label, condition, start, end) ->
                    new JobPart("Copy FOLIO user data for users " + label) {
                      @Override
                      protected void execute() {
                        // fetch all FOLIO IDs to inspect
                        Map<UUID, String> folioIdToKeycloakId =
                          this.createKeycloak()
                            .select(matchedFolioId.cast(UUID.class), matchedKeycloakId)
                            .from(tempMatchedUserIds)
                            .where(condition)
                            .fetchMap(matchedFolioId.cast(UUID.class), matchedKeycloakId);

                        // fetch associated data (no cross-DB calls allowed)
                        List<Record5<UUID, String, String, String, String>> folioUserData =
                          this.create()
                            .select(
                              field("id", UUID.class),
                              field("jsonb->>'username'", String.class),
                              field("jsonb->'personal'->>'email'", String.class),
                              field("jsonb->'personal'->>'firstName'", String.class),
                              field("jsonb->'personal'->>'lastName'", String.class)
                            )
                            .from(new TableReference("users", "users").table(ctx.tenant().tenant()))
                            .where(field("id", UUID.class).in(folioIdToKeycloakId.keySet()))
                            .fetch();

                        // store into final table
                        List<Query> queries = folioUserData
                          .stream()
                          .map(record ->
                            (Query) this.createKeycloak()
                              .insertInto(tempUserDataTable)
                              .columns(
                                dataKeycloakId,
                                dataFolioId,
                                dataUsername,
                                dataEmail,
                                dataFirstName,
                                dataLastName
                              )
                              .values(
                                folioIdToKeycloakId.get(record.get("id", UUID.class)),
                                record.get("id", UUID.class).toString(),
                                record.get("jsonb->>'username'", String.class),
                                record.get("jsonb->'personal'->>'email'", String.class),
                                record.get("jsonb->'personal'->>'firstName'", String.class),
                                record.get("jsonb->'personal'->>'lastName'", String.class)
                              )
                          )
                          .toList();

                        for (int i = 0; i < queries.size(); i += JobConfig.INSERT_BATCH_SIZE) {
                          int end = Math.min(i + JobConfig.INSERT_BATCH_SIZE, queries.size());
                          List<Query> batch = queries.subList(i, end);
                          this.createKeycloak().batch(batch).execute();
                        }
                      }
                    }
                )
                  .withKeycloakDb()
              )
            );
          }

          if (JobConfigurationProperty.isOn(ctx.settings(), "reset-credentials")) {
            job.scheduleParts(
              "apply-new-values-prep",
              List.of(
                new BatchGenerationFromTablePart<>(
                  "Make batches to overwrite user credentials",
                  dataKeycloakIdFR,
                  String.class,
                  JobConfig.BATCH_SIZE,
                  "apply-new-values",
                  (label, condition, start, end) ->
                    new JobPart("Set KC credentials to 'folio' for users " + label) {
                      @Override
                      protected void execute() {
                        Stream
                          .of("credential", "fed_user_credential")
                          .forEach(table ->
                            this.createKeycloak()
                              .update(table(name("public", table)))
                              .set(
                                field("user_label", String.class),
                                translate(
                                  field("{0}", String.class, field("user_label", String.class)),
                                  RandomValueUtils.POSTGRES_TRANSLATE_FROM,
                                  RandomValueUtils.POSTGRES_TRANSLATE_TO
                                )
                              )
                              .set(
                                field("secret_data", String.class),
                                "{\"value\":\"rtCTLgeYthWGZnZJQZIB043T5lbmrT5ooIBREKHu1So=\",\"salt\":\"WQEjDm8FdaAUcJRo1dqvoQ\",\"additionalParameters\":{}}"
                              )
                              .set(
                                field("credential_data", String.class),
                                "{\"hashIterations\":5,\"algorithm\":\"argon2\",\"additionalParameters\":{\"hashLength\":[\"32\"],\"memory\":[\"7168\"],\"type\":[\"id\"],\"version\":[\"1.3\"],\"parallelism\":[\"1\"]}}"
                              )
                              .where(userId.in(select(dataKeycloakId).from(tempUserDataTable).where(condition)))
                              .execute()
                          );
                      }
                    }
                )
                  .withKeycloakDb()
              )
            );
          }

          if (JobConfigurationProperty.isOn(ctx.settings(), "reset-idp")) {
            job.scheduleParts(
              "apply-new-values-prep",
              List.of(
                new BatchGenerationFromTablePart<>(
                  "Make batches to overwrite IDP external IDs",
                  dataKeycloakIdFR,
                  String.class,
                  JobConfig.BATCH_SIZE,
                  "apply-new-values",
                  (label, condition, start, end) ->
                    new JobPart("Set IDP external username/ID to FOLIO username for users " + label) {
                      @Override
                      protected void execute() {
                        Stream
                          .of("broker_link", "federated_identity")
                          .forEach(table -> {
                            String prefix = StringUtils.split(table, "_")[0] + "_";
                            this.createKeycloak()
                              .update(table(name("public", table)))
                              .set(
                                field(name(prefix + "username"), String.class),
                                select(dataUsername).from(tempUserDataTable).where(dataKeycloakId.eq(userId))
                              )
                              .set(
                                field(name(prefix + "user_id"), String.class),
                                select(dataUsername).from(tempUserDataTable).where(dataKeycloakId.eq(userId))
                              )
                              .where(userId.in(select(dataKeycloakId).from(tempUserDataTable).where(condition)))
                              .execute();
                          });
                      }
                    }
                )
                  .withKeycloakDb()
              )
            );
          }

          if (JobConfigurationProperty.isOn(ctx.settings(), "update-user-info")) {
            job.scheduleParts(
              "apply-new-values-prep",
              List.of(
                new BatchGenerationFromTablePart<>(
                  "Make batches to update KC user data",
                  dataKeycloakIdFR,
                  String.class,
                  JobConfig.BATCH_SIZE,
                  "apply-new-values",
                  (label, condition, start, end) ->
                    new JobPart("Sync user data with FOLIO data for users " + label) {
                      @Override
                      protected void execute() {
                        this.createKeycloak()
                          .update(keycloakUserEntityTable)
                          .set(
                            field("username", String.class),
                            select(dataUsername)
                              .from(tempUserDataTable)
                              .where(dataKeycloakId.eq(keycloakUserEntityIdField))
                          )
                          .set(
                            field("email", String.class),
                            select(dataEmail)
                              .from(tempUserDataTable)
                              .where(dataKeycloakId.eq(keycloakUserEntityIdField))
                          )
                          .set(
                            field("email_constraint", String.class),
                            // only update if it is an email (sometimes it is UUID)
                            select(
                              field(
                                "case when {0} like '%@%' then {1} else {0} end",
                                String.class,
                                field("email_constraint", String.class),
                                dataEmail
                              )
                            )
                              .from(tempUserDataTable)
                              .where(dataKeycloakId.eq(keycloakUserEntityIdField))
                          )
                          .set(
                            field("first_name", String.class),
                            select(dataFirstName)
                              .from(tempUserDataTable)
                              .where(dataKeycloakId.eq(keycloakUserEntityIdField))
                          )
                          .set(
                            field("last_name", String.class),
                            select(dataLastName)
                              .from(tempUserDataTable)
                              .where(dataKeycloakId.eq(keycloakUserEntityIdField))
                          )
                          .where(
                            keycloakUserEntityIdField.in(
                              select(dataKeycloakId).from(tempUserDataTable).where(condition)
                            )
                          )
                          .execute();
                      }
                    }
                )
                  .withKeycloakDb()
              )
            );
          }

          // teardown
          if (JobConfigurationProperty.isOn(ctx.settings(), "drop-table")) {
            job.scheduleParts(
              "cleanup",
              List.of(
                new DropTablePart("Drop matched user ID temporary table", tempMatchedUserIds).withKeycloakDb(),
                new DropTablePart("Drop user data temporary table", tempUserDataTable).withKeycloakDb()
              )
            );
          }

          return job;
        }
      )
    );
  }

  private List<JobConfigurationProperty> getProperties(boolean isKeycloakConfigured, Pair<String, String>... options) {
    return Stream
      .of(options)
      .map(option -> {
        if (isKeycloakConfigured) {
          return new JobConfigurationProperty(option.getLeft(), option.getRight(), true, false);
        } else {
          return new JobConfigurationProperty(
            option.getLeft(),
            row(text(option.getRight()).crossedOut(), spacer(1), text("(not available in this environment)").italic()),
            true,
            true
          );
        }
      })
      .toList();
  }
}
