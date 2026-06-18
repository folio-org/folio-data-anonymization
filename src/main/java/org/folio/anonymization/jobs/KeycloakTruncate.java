package org.folio.anonymization.jobs;

import static dev.tamboui.toolkit.Toolkit.row;
import static dev.tamboui.toolkit.Toolkit.spacer;
import static dev.tamboui.toolkit.Toolkit.text;
import static org.jooq.impl.DSL.name;

import java.util.List;
import java.util.stream.Stream;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobContext;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.TruncateJooqTablePart;
import org.folio.anonymization.repository.UtilRepository;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class KeycloakTruncate implements JobFactory {

  @Autowired
  private SharedExecutionContext context;

  @Autowired
  private UtilRepository utilRepository;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    boolean isKeycloakConfigured = utilRepository.doesKeycloakExist();

    return List.of(
      new JobBuilder(
        "keycloak_truncate_logs",
        "[Keycloak] Truncate Keycloak logs (all tenants)",
        "Truncates logging and audit tables that cannot be safely anonymized, affects all tenants in env",
        tenant,
        context,
        getTableProperties(isKeycloakConfigured, "admin_event_entity", "event_entity"),
        ctx -> new Job(ctx, List.of("truncate")).scheduleParts("truncate", getParts(ctx))
      ),
      new JobBuilder(
        "keycloak_truncate_sessions",
        "[Keycloak] Truncate Keycloak session tables (all tenants)",
        "Truncates offline session tables, affects all tenants in env",
        tenant,
        context,
        getTableProperties(isKeycloakConfigured, "offline_client_session", "offline_user_session"),
        ctx -> new Job(ctx, List.of("truncate")).scheduleParts("truncate", getParts(ctx))
      ),
      new JobBuilder(
        "keycloak_truncate_smtp_config",
        "[Keycloak] Truncate Keycloak SMTP configuration (all tenants)",
        "Truncates SMTP configuration (plain credentials) used by Keycloak, affects all tenants in env",
        tenant,
        context,
        getTableProperties(isKeycloakConfigured, "realm_smtp_config"),
        ctx -> new Job(ctx, List.of("truncate")).scheduleParts("truncate", getParts(ctx))
      )
    );
  }

  private List<TruncateJooqTablePart> getParts(JobContext ctx) {
    return ctx
      .settings()
      .stream()
      .filter(JobConfigurationProperty::isOn)
      .map(JobConfigurationProperty::getKey)
      .map(String.class::cast)
      .map(table ->
        new TruncateJooqTablePart(table, ctx.executionContext().createKeycloak(), DSL.table(name("public", table)))
      )
      .toList();
  }

  private List<JobConfigurationProperty> getTableProperties(boolean isKeycloakConfigured, String... tables) {
    return Stream
      .of(tables)
      .map(table -> {
        if (isKeycloakConfigured) {
          return new JobConfigurationProperty(table, table, true, false);
        } else {
          return new JobConfigurationProperty(
            table,
            row(text(table.toString()).crossedOut(), spacer(1), text("(not available in this environment)").italic()),
            true,
            true
          );
        }
      })
      .toList();
  }
}
