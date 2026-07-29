package org.folio.anonymization.jobs;

import static dev.tamboui.toolkit.Toolkit.row;
import static dev.tamboui.toolkit.Toolkit.spacer;
import static dev.tamboui.toolkit.Toolkit.text;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.selectOne;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.tuple.Pair;
import org.folio.anonymization.config.JobConfig;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.folio.Tenant;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.JobPart;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.BatchGenerationFromTablePart;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Table;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Log4j2
@Component
@SuppressWarnings("unchecked")
public class ConsortiumShadowSync implements JobFactory {

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    boolean isEcs = !tenant.consortiumSiblings().isEmpty();

    return List.of(
      new JobBuilder(
        "shadow_sync",
        "[ECS Only] Synchronize shadow user personal data from main tenant",
        "Synchronizes jsonb->personal from main tenants for each shadow user. Does nothing on non-ECS tenants.",
        tenant,
        context,
        getProperties(
          isEcs,
          Pair.of(
            "update-user-info",
            "Sync user personal info (name, email, etc) to match the original tenant's anonymized data"
          )
        ),
        ctx -> {
          Job job = new Job(ctx, List.of("propagate-shadow-users-prep", "propagate-shadow-users"));

          List<Tenant> siblingsBesidesUs = tenant
            .consortiumSiblings()
            .stream()
            .filter(s -> !s.id().equals(tenant.tenant().id()))
            .toList();

          // note that we only care about these five fields, per https://github.com/folio-org/mod-users/blob/dafb49b39caacc3c164169be878d699d49257a2b/src/main/java/org/folio/service/UsersService.java#L57-L63
          // first name, last name, phone, mobile phone, email
          job.scheduleParts(
            "propagate-shadow-users-prep",
            siblingsBesidesUs
              .stream()
              .map(sibling ->
                new BatchGenerationFromTablePart<>(
                  "Make batches to propagate shadow user data from " + sibling.id(),
                  new FieldReference("users", "users", "id"),
                  UUID.class,
                  JobConfig.BATCH_SIZE,
                  "propagate-shadow-users",
                  // copy data from the sibling's temp table to the current tenant's temp table
                  (l, condition, start, end) ->
                    new JobPart("Propagate shadow user data from " + sibling.id() + " on " + l) {
                      @Override
                      protected void execute() {
                        //   select thisu.jsonb->'personal' as thisval,
                        //     sibu.jsonb->'personal' as sibval,
                        //     sibu.jsonb as sibjson,
                        //     thisu.jsonb as thisjson
                        //   from cs00000int_0011_mod_users.users thisu
                        //   join cs00000int_mod_users.users sibu on thisu.id = sibu.id
                        //   where thisu.jsonb->>'type' = 'shadow' -- already included in userCondition
                        //   and sibu.jsonb->>'type' <> 'shadow'
                        FieldReference id = new FieldReference("users", "users", "id");
                        FieldReference jsonb = new FieldReference("users", "users", "jsonb");

                        Table<?> ourUsersTable = id.table(tenant.tenant());
                        Table<?> siblingUsersTable = id.table(sibling);
                        Field<UUID> ourUserIdField = id.baseColumn(tenant.tenant(), UUID.class);
                        Field<UUID> siblingUserIdField = id.baseColumn(sibling, UUID.class);
                        Field<String> ourUserType = field(
                          "{0}->>'type'",
                          String.class,
                          jsonb.baseColumn(tenant.tenant(), JSONB.class)
                        );
                        Field<String> siblingUserType = field(
                          "{0}->>'type'",
                          String.class,
                          jsonb.baseColumn(sibling, JSONB.class)
                        );

                        this.create()
                          .update(ourUsersTable)
                          .set(
                            jsonb.baseColumn(tenant.tenant(), JSONB.class),
                            field(
                              """
                              jsonb_set(
                                {0},
                                '{personal}',
                                COALESCE({0}->'personal', '{}'::jsonb) || jsonb_build_object(
                                  'lastName', {1}->'lastName',
                                  'firstName', {1}->'firstName',
                                  'middleName', {1}->'middleName',
                                  'preferredFirstName', {1}->'preferredFirstName',
                                  'email', {1}->'email',
                                  'phone', {1}->'phone',
                                  'mobilePhone', {1}->'mobilePhone'
                                )
                              )
                              """,
                              JSONB.class,
                              jsonb.baseColumn(tenant.tenant(), JSONB.class),
                              field("replacement.personal", JSONB.class)
                            )
                          )
                          .from(
                            select(
                              ourUserIdField.as("id"),
                              field("{0}->'personal'", JSONB.class, jsonb.baseColumn(sibling, JSONB.class))
                                .as("personal")
                            )
                              .from(ourUsersTable)
                              .join(siblingUsersTable)
                              .on(ourUserIdField.eq(siblingUserIdField))
                              .where(condition.and(siblingUserType.ne("shadow")))
                              .asTable("replacement")
                          )
                          .where(id.baseColumn(this.tenant(), UUID.class).eq(field("replacement.id", UUID.class)))
                          .execute();
                      }
                    },
                  // see notes in ShadowUserPropagationBatchPart on why we can't just use the original tenant ID
                  field(
                    "COALESCE({0}->>'type', '')",
                    String.class,
                    new FieldReference("users", "users", "jsonb").baseColumn(tenant.tenant(), JSONB.class)
                  )
                    .equal("shadow")
                )
              )
              .toList()
          );

          return job;
        }
      )
    );
  }

  private List<JobConfigurationProperty> getProperties(boolean isEcs, Pair<String, String>... options) {
    return Stream
      .of(options)
      .map(option -> {
        if (isEcs) {
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
