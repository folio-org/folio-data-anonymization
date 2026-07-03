package org.folio.anonymization.jobs;

import static dev.tamboui.toolkit.Toolkit.row;
import static dev.tamboui.toolkit.Toolkit.spacer;
import static dev.tamboui.toolkit.Toolkit.text;
import static org.jooq.impl.DSL.exists;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.selectOne;
import static org.jooq.impl.DSL.table;

import java.util.List;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.JobPart;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.repository.UtilRepository;
import org.folio.anonymization.util.RandomValueUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class KeycloakRoleDescriptionRedaction implements JobFactory {

  @Autowired
  private SharedExecutionContext context;

  @Autowired
  private UtilRepository utilRepository;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    boolean isKeycloakConfigured = utilRepository.doesKeycloakExist();

    return List.of(
      new JobBuilder(
        "keycloak_redact_role_descriptions",
        "[Keycloak] Redact Keycloak role descriptions",
        "Redacts role descriptions in Keycloak (FOLIO side is under 'Free text labels and descriptions' job)",
        tenant,
        context,
        List.of(
          isKeycloakConfigured
            ? new JobConfigurationProperty("perform-redaction", "Redact Keycloak role descriptions", true, false)
            : new JobConfigurationProperty(
              "perform-redaction",
              row(
                text("Redact Keycloak role descriptions").crossedOut(),
                spacer(1),
                text("(not available in this environment)").italic()
              ),
              true,
              true
            )
        ),
        ctx -> {
          Job job = new Job(ctx, List.of("redact"));

          if (ctx.settings().get(0).isOn()) {
            // few of these, so no need to batch
            job.scheduleParts(
              "redact",
              List.of(
                new JobPart("Redact Keycloak role descriptions") {
                  @Override
                  protected void execute() {
                    this.createKeycloak()
                      .update(table(name("keycloak_role")).as("rl"))
                      .set(
                        field("description", String.class),
                        field(
                          """
                          (
                            SELECT string_agg(
                              CASE
                                WHEN part ~ '^\\$\\{'
                                  THEN part
                                  ELSE translate(part, '%s', '%s')
                              END,
                              '' ORDER BY idx
                            )
                            FROM (
                              SELECT m[1] AS part, row_number() OVER () AS idx
                              FROM regexp_matches(
                                rl.description,
                                '\\$\\{[^}]*(?:\\}|$)|(?:(?!\\$\\{).)+',
                                'g'
                              ) AS m
                            ) t
                          )
                        """.formatted(
                              RandomValueUtils.POSTGRES_TRANSLATE_FROM,
                              RandomValueUtils.POSTGRES_TRANSLATE_TO
                            ),
                          String.class
                        )
                      )
                      // check that corresponding realm table associates this role as realm foo
                      .where(
                        exists(
                          selectOne()
                            .from(table(name("realm")).as("rm"))
                            .where(
                              field("rl.realm_id", String.class)
                                .eq(field("rm.id", String.class))
                                .and(field("rm.name", String.class).eq(tenant.tenant().id()))
                            )
                        )
                      )
                      .execute();
                  }
                }
              )
            );
          }

          return job;
        }
      )
    );
  }
}
