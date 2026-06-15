package org.folio.anonymization.jobs;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.trueCondition;

import java.util.List;
import java.util.stream.Stream;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.ReplaceJSONBValuePart;
import org.folio.anonymization.service.SeedFileService;
import org.folio.anonymization.util.RandomValueUtils;
import org.jooq.JSONB;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class SAMLConfigurationAnonymization implements JobFactory {

  private static final FieldReference CONFIGURATION_FIELD = new FieldReference("login_saml", "configuration", "jsonb");

  @Autowired
  private SharedExecutionContext context;

  @Autowired
  private SeedFileService seedFileService;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "saml_configuration",
        "SAML configuration anonymization",
        "Replaces SAML private keys in mod_login_saml",
        tenant,
        context,
        JobConfigurationProperty.fromFieldList(List.of(CONFIGURATION_FIELD), tenant),
        ctx ->
          new Job(ctx, List.of("overwrite"))
            .scheduleParts(
              "overwrite",
              JobConfigurationProperty
                .getEnabledFields(ctx.settings())
                .flatMap(field ->
                  Stream.of(
                    new ReplaceJSONBValuePart(
                      "Replace SAML keystore",
                      CONFIGURATION_FIELD.withJsonPath("$.['keystore.file']"),
                      trueCondition(),
                      field(
                        "to_jsonb({0}::text)",
                        JSONB.class,
                        RandomValueUtils.encodeBase64(seedFileService.getSeedFilesAsBytes("saml.jks").get(0))
                      )
                    ),
                    new ReplaceJSONBValuePart(
                      "Replace SAML keystore main password",
                      CONFIGURATION_FIELD.withJsonPath("$.['keystore.password']"),
                      trueCondition(),
                      field("to_jsonb({0}::text)", JSONB.class, "folio")
                    ),
                    new ReplaceJSONBValuePart(
                      "Replace SAML keystore private key password",
                      CONFIGURATION_FIELD.withJsonPath("$.['keystore.privatekey.password']"),
                      trueCondition(),
                      field("to_jsonb({0}::text)", JSONB.class, "folio")
                    )
                  )
                )
                .toList()
            )
      )
    );
  }
}
