package org.folio.anonymization.jobs;

import static dev.tamboui.toolkit.Toolkit.row;
import static dev.tamboui.toolkit.Toolkit.text;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.not;
import static org.jooq.impl.DSL.translate;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.tuple.Pair;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.DeletePart;
import org.folio.anonymization.jobs.templates.RedactPart;
import org.folio.anonymization.jobs.templates.ReplaceJSONBValuePart;
import org.folio.anonymization.service.SeedFileService;
import org.folio.anonymization.util.DBUtils;
import org.folio.anonymization.util.RandomValueUtils;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Record3;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class ConfigurationAnonymization implements JobFactory {

  private static final FieldReference CONFIGURATION_FIELD = new FieldReference("configuration", "config_data", "jsonb");
  private static final FieldReference SETTINGS_FIELD = new FieldReference("settings", "settings", "value");

  @Autowired
  private SharedExecutionContext context;

  @Autowired
  private SeedFileService seedFileService;

  @SuppressWarnings("unchecked")
  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    byte[] keystore = seedFileService.getSeedFilesAsBytes("saml.jks").get(0);

    List<JobConfigurationProperty> options = new ArrayList<>();
    if (
      tenant
        .availableTables()
        .stream()
        .anyMatch(t -> t.schema().equals(CONFIGURATION_FIELD.schema()) && t.table().equals(CONFIGURATION_FIELD.table()))
    ) {
      Field<String> module = field(
        "{0}->>'module'",
        String.class,
        CONFIGURATION_FIELD.baseColumn(tenant.tenant(), JSONB.class)
      );
      Field<String> configName = field(
        "{0}->>'configName'",
        String.class,
        CONFIGURATION_FIELD.baseColumn(tenant.tenant(), JSONB.class)
      );
      Field<String> code = field(
        "{0}->>'code'",
        String.class,
        CONFIGURATION_FIELD.baseColumn(tenant.tenant(), JSONB.class)
      );

      List<Record3<String, String, String>> foundConfigs =
        this.context.create()
          .select(module, configName, code)
          .from(CONFIGURATION_FIELD.table(tenant.tenant()))
          .where(
            // verified to be safe
            not(
              module.in(
                "AGREEMENTS",
                "BULKEDIT",
                "edge-sip2",
                "ERM-USAGE-HARVESTER",
                "FAST_ADD",
                "INVOICE",
                "ORDERS",
                "PLUGINS",
                "USERS",
                "USERSBL"
              )
            )
              .andNot(module.eq("ORG").and(configName.eq("bindings")))
              .andNot(module.eq("@folio/stripes-core").and(configName.eq("localeSettings")))
              .andNot(module.eq("SETTINGS").and(configName.in("locale", "TLR", "PRINT_HOLD_REQUESTS")))
              .andNot(module.eq("OAIPMH").and(configName.notEqual("general")))
              .andNot(module.eq("LOAN_HISTORY").and(configName.eq("loan_history")))
              .andNot(
                module
                  .eq("LOGIN-SAML-SSOCircle")
                  .and(configName.eq("saml"))
                  .and(
                    code.in(
                      "idp.url",
                      "metadata.invalidated",
                      "okapi.url",
                      "saml.attribute",
                      "saml.binding",
                      "user.property"
                    )
                  )
              )
              .andNot(module.eq("AES").and(configName.eq("routing_rules")))
              .andNot(module.eq("COURSES").and(configName.eq("display")))
              .andNot(
                module
                  .eq("CHECKOUT")
                  .and(
                    configName.in(
                      "TLR",
                      "regularTlr",
                      "generalTlr",
                      "PRINT_HOLD_REQUESTS",
                      "noticesLimit",
                      "other_settings",
                      "loan_history"
                    )
                  )
              )
          )
          .fetch()
          .stream()
          .collect(Collectors.toCollection(ArrayList::new));

      options.add(
        new JobConfigurationProperty(
          (Consumer<Job>) job ->
            job.scheduleParts(
              "act",
              List.of(
                new ReplaceJSONBValuePart(
                  "[mod-config] Redact tenant addresses",
                  CONFIGURATION_FIELD.withJsonPath("$.value"),
                  module.eq("TENANT").and(configName.eq("tenant.addresses")),
                  val ->
                    field(
                      "to_jsonb(jsonb_set_lax(({0})::jsonb, '{address}', to_jsonb({1}), false, 'return_target')::text)",
                      JSONB.class,
                      DBUtils.jsonbToString(val),
                      translate(
                        field(
                          "unaccent({0})",
                          String.class,
                          DBUtils.resolveFieldPropertiesToString(
                            field("({0})::jsonb", JSONB.class, DBUtils.jsonbToString(val)),
                            List.of("address")
                          )
                        ),
                        RandomValueUtils.POSTGRES_TRANSLATE_FROM,
                        RandomValueUtils.POSTGRES_TRANSLATE_TO
                      )
                    )
                )
              )
            ),
          "[mod-configuration] Redact tenant addresses",
          true,
          !findAndRemoveFromConfigurationList(
            foundConfigs,
            r -> r.get(module).equals("TENANT") && r.get(configName).equals("tenant.addresses")
          )
        )
      );
      options.add(
        new JobConfigurationProperty(
          (Consumer<Job>) job ->
            job.scheduleParts(
              "act",
              List.of(
                new ReplaceJSONBValuePart(
                  "[mod-config] Anonymize mod-oai-pmh administratorEmail",
                  CONFIGURATION_FIELD.withJsonPath("$.value"),
                  module.eq("OAIPMH").and(configName.eq("general")),
                  val ->
                    field(
                      "to_jsonb(jsonb_set_lax(({0})::jsonb, '{administratorEmail}', to_jsonb({1}), false, 'return_target')::text)",
                      JSONB.class,
                      DBUtils.jsonbToString(val),
                      RandomValueUtils.emails(1).get(0)
                    )
                )
              )
            ),
          "[mod-configuration] Anonymize mod-oai-pmh administrator email",
          true,
          !findAndRemoveFromConfigurationList(
            foundConfigs,
            r -> r.get(module).equals("OAIPMH") && r.get(configName).equals("general")
          )
        )
      );
      options.add(
        new JobConfigurationProperty(
          (Consumer<Job>) job ->
            job.scheduleParts(
              "act",
              List.of(
                new RedactPart(
                  "[mod-config] Redact SMTP credentials and host",
                  CONFIGURATION_FIELD.withJsonPath("$.value"),
                  module
                    .eq("SMTP_SERVER")
                    .and(configName.eq("email"))
                    .and(
                      code.in("EMAIL_FROM", "EMAIL_USERNAME", "EMAIL_PASSWORD", "EMAIL_SMTP_HOST", "EMAIL_SMTP_PORT")
                    )
                )
              )
            ),
          "[mod-configuration] Redact SMTP server credentials/host",
          true,
          !findAndRemoveFromConfigurationList(
            foundConfigs,
            r -> r.get(module).equals("SMTP_SERVER") && r.get(configName).equals("email")
          )
        )
      );
      options.add(
        new JobConfigurationProperty(
          (Consumer<Job>) job ->
            job.scheduleParts(
              "act",
              List.of(
                new ReplaceJSONBValuePart(
                  "[mod-config] Replace SAML keystore",
                  CONFIGURATION_FIELD.withJsonPath("$.value"),
                  module.eq("LOGIN-SAML-SSOCircle").and(configName.eq("saml")).and(code.eq("keystore.file")),
                  field("to_jsonb({0}::text)", JSONB.class, RandomValueUtils.encodeBase64(keystore))
                ),
                new ReplaceJSONBValuePart(
                  "[mod-config] Replace SAML keystore passwords",
                  CONFIGURATION_FIELD.withJsonPath("$.value"),
                  module
                    .eq("LOGIN-SAML-SSOCircle")
                    .and(configName.eq("saml"))
                    .and(code.in("keystore.password", "keystore.privatekey.password")),
                  field("to_jsonb({0}::text)", JSONB.class, "folio")
                )
              )
            ),
          "[mod-configuration] Replace SAML keystore",
          true,
          !findAndRemoveFromConfigurationList(
            foundConfigs,
            r ->
              r.get(module).equals("LOGIN-SAML-SSOCircle") &&
              r.get(configName).equals("saml") &&
              List.of("keystore.file", "keystore.password", "keystore.privatekey.password").contains(r.get(code))
          )
        )
      );
      options.add(
        new JobConfigurationProperty(
          (Consumer<Job>) job ->
            job.scheduleParts(
              "act",
              List.of(
                new DeletePart(
                  "[mod-config] Remove mod-data-export-spring configurations",
                  CONFIGURATION_FIELD.tableReference(),
                  module.eq("mod-data-export-spring")
                )
              )
            ),
          "[mod-configuration] Remove mod-data-export-spring configurations (may contain FTP credentials)",
          true,
          !findAndRemoveFromConfigurationList(foundConfigs, r -> r.get(module).equals("mod-data-export-spring"))
        )
      );

      log.warn("Orphaned configuration entries: {}", foundConfigs);

      options.addAll(
        foundConfigs
          .stream()
          .map(r -> Pair.of(r.get(module), r.get(configName)))
          .distinct()
          .map(p ->
            new JobConfigurationProperty(
              (Consumer<Job>) job ->
                job.scheduleParts(
                  "act",
                  List.of(
                    new DeletePart(
                      "[mod-config] Remove configuration entries for module=" +
                      p.getLeft() +
                      " configName=" +
                      p.getRight(),
                      CONFIGURATION_FIELD.tableReference(),
                      module.eq(p.getLeft()).and(configName.eq(p.getRight()))
                    )
                  )
                ),
              row(
                text("[mod-configuration] "),
                text("(unknown configuration type)").yellow(),
                text(" Remove configuration entries for module=" + p.getLeft() + " configName=" + p.getRight())
              ),
              false,
              false
            )
          )
          .toList()
      );
    }

    if (
      tenant
        .availableTables()
        .stream()
        .anyMatch(t -> t.schema().equals(SETTINGS_FIELD.schema()) && t.table().equals(SETTINGS_FIELD.table()))
    ) {
      Field<String> scope = SETTINGS_FIELD.withColumn("scope").baseColumn(tenant.tenant(), String.class);
      Field<String> key = SETTINGS_FIELD.withColumn("key").baseColumn(tenant.tenant(), String.class);

      options.addAll(
        context
          .create()
          .selectDistinct(scope)
          .from(SETTINGS_FIELD.table(tenant.tenant()))
          .where(
            not(scope.eq("ui-inventory.number-generator-settings.manage").and(key.eq("number-generator-settings")))
              .andNot(scope.eq("ui-users.custom-fields-label.manage").and(key.eq("custom_fields_label")))
              .andNot(scope.eq("mod-ncip").and(key.in("rapid", "reshare")))
          )
          .fetch()
          .map(r ->
            new JobConfigurationProperty(
              (Consumer<Job>) job ->
                job.scheduleParts(
                  "act",
                  List.of(
                    new DeletePart(
                      "[mod-settings] Remove settings with scope=" + r.value1(),
                      SETTINGS_FIELD.tableReference(),
                      scope.eq(r.value1())
                    )
                  )
                ),
              row(
                text("[mod-settings] "),
                text("(unknown setting type)").yellow(),
                text(" Remove settings with scope=" + r.value1())
              ),
              false,
              false
            )
          )
      );
    }

    return List.of(
      new JobBuilder(
        "Global configuration anonymization",
        "Handle values from mod-configuration and mod-settings",
        tenant,
        context,
        options,
        ctx -> {
          Job job = new Job(ctx, List.of("act"));
          ctx
            .settings()
            .stream()
            .filter(JobConfigurationProperty::isOn)
            .map(JobConfigurationProperty::getKey)
            .map(f -> (Consumer<Job>) f)
            .forEach(f -> f.accept(job));
          return job;
        }
      )
    );
  }

  public static boolean findAndRemoveFromConfigurationList(
    List<Record3<String, String, String>> list,
    Predicate<Record3<String, String, String>> predicate
  ) {
    return list.removeIf(predicate);
  }
}
