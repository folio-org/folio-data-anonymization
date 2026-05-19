package org.folio.anonymization.jobs;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.primaryKey;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.DSL.unique;

import java.util.List;
import java.util.stream.Stream;
import org.folio.anonymization.config.JobConfig;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.BatchGenerationFromSequencePart;
import org.folio.anonymization.jobs.templates.BatchGenerationFromTablePart;
import org.folio.anonymization.jobs.templates.CreateTablePart;
import org.folio.anonymization.jobs.templates.DropTablePart;
import org.folio.anonymization.jobs.templates.ExcludeGeneratedValuesPart;
import org.folio.anonymization.jobs.templates.FindSystemUsersPart;
import org.folio.anonymization.jobs.templates.GenerateValuesPart;
import org.folio.anonymization.jobs.templates.InsertIntoTablePart;
import org.folio.anonymization.jobs.templates.ReplaceJSONBValuePart;
import org.folio.anonymization.jobs.templates.ReplaceValuePart;
import org.folio.anonymization.util.DBUtils;
import org.folio.anonymization.util.RandomValueUtils;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Table;
import org.jooq.impl.SQLDataType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class UserExternalSystemIdAnonymization implements JobFactory {

  // special handling to capture system user ones from here that should not be anonymized
  private static final FieldReference USERS_TABLE_FIELD = new FieldReference(
    "users",
    "users",
    "jsonb",
    "$.externalSystemId"
  );

  private static final List<FieldReference> FIELDS = List.of(
    new FieldReference("oa", "party", "p_orcid_id"),
    new FieldReference("users", "staging_users", "jsonb", "$.externalSystemId"),
    new FieldReference("users", "user_tenant", "external_system_id"),
    USERS_TABLE_FIELD
  );

  @Autowired
  private SharedExecutionContext context;

  @SuppressWarnings("unchecked")
  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "User external system ID anonymization",
        "Replaces user external system IDs with unique generated values.",
        tenant,
        context,
        Stream
          .concat(
            Stream.of(
              new JobConfigurationProperty(
                "create-table",
                "Create temporary table with current and new values (disable if resuming a previous run)"
              ),
              new JobConfigurationProperty(
                "drop-table",
                "Destroy temporary table (PII will be left behind if disabled)"
              )
            ),
            JobConfigurationProperty.fromFieldList(FIELDS, tenant).stream()
          )
          .toList(),
        ctx -> {
          Table<?> tempTableFinal = table(
            name("public", "_danon_" + ctx.tenant().tenant().id() + "_user_external_system_ids")
          );
          Table<?> tempTableStaging = table(
            name("public", "_danon_" + ctx.tenant().tenant().id() + "_user_external_system_ids_staging")
          );

          Field<String> originalValue = field("original_value", SQLDataType.VARCHAR.notNull());
          Field<String> newValue = field("new_value", SQLDataType.VARCHAR.null_());

          Job job = new Job(
            ctx,
            List.of(
              "prepare",
              "enumerate-prep",
              "enumerate",
              "generate-new-values-prep",
              "generate-new-values",
              "exclude-system-user-values-prep",
              "exclude-system-user-values",
              "apply-new-values-prep",
              "apply-new-values",
              "cleanup"
            )
          );

          if (JobConfigurationProperty.isOn(ctx.settings(), "create-table")) {
            job.scheduleParts(
              "prepare",
              List.of(
                new CreateTablePart(
                  "Create temporary table (staging)",
                  tempTableStaging,
                  List.of(originalValue),
                  List.of(primaryKey(originalValue)),
                  true
                ),
                new CreateTablePart(
                  "Create temporary table (final)",
                  tempTableFinal,
                  List.of(originalValue, newValue),
                  List.of(primaryKey(originalValue), unique(newValue)),
                  false
                )
              )
            );
            job.scheduleParts(
              "enumerate-prep",
              JobConfigurationProperty
                .getEnabledFields(ctx.settings())
                .map(field ->
                  new BatchGenerationFromTablePart<>(
                    "Make batches to enumerate data from " + field.toString(),
                    field,
                    JobConfig.BATCH_SIZE,
                    "enumerate",
                    (label, condition, start, end) ->
                      new InsertIntoTablePart(
                        "Enumerate data from " + field.toString() + " " + label,
                        tempTableStaging,
                        select(field("a"))
                          .from(
                            select(field.field(ctx.tenant().tenant(), String.class).as("a"))
                              .from(field.table(ctx.tenant().tenant()))
                              .where(condition)
                          )
                          .where(field("a").isNotNull())
                      )
                  )
                )
                .toList()
            );

            job.scheduleParts(
              "generate-new-values-prep",
              List.of(
                new BatchGenerationFromSequencePart(
                  "Analyze table size for split processing",
                  tempTableStaging,
                  JobConfig.BATCH_SIZE,
                  "generate-new-values",
                  (label, cond, start, end) ->
                    new GenerateValuesPart(
                      "Generate values %s".formatted(label),
                      tempTableFinal,
                      newValue,
                      select(originalValue).from(tempTableStaging).where(cond),
                      RandomValueUtils.codeLikeValueGenerator(start)
                    )
                )
              )
            );
          }

          job.scheduleParts(
            "exclude-system-user-values-prep",
            List.of(
              new FindSystemUsersPart(
                "Find system user values",
                USERS_TABLE_FIELD.table(tenant.tenant()),
                field(
                  "{0}->>'externalSystemId'",
                  String.class,
                  USERS_TABLE_FIELD.baseColumn(tenant.tenant(), JSONB.class)
                ),
                systemUserValues ->
                  job.scheduleParts(
                    "exclude-system-user-values",
                    List.of(
                      new ExcludeGeneratedValuesPart(
                        "Exclude system user values from anonymization",
                        tempTableFinal,
                        originalValue,
                        newValue,
                        systemUserValues
                      )
                    )
                  )
              )
            )
          );

          job.scheduleParts(
            "apply-new-values-prep",
            JobConfigurationProperty
              .getEnabledFields(ctx.settings())
              .map(field ->
                new BatchGenerationFromTablePart<>(
                  "Prep to apply new values to " + field.toString(),
                  field,
                  JobConfig.BATCH_SIZE,
                  "apply-new-values",
                  (label, condition, start, end) -> {
                    if (field.jsonPath() != null) {
                      return new ReplaceJSONBValuePart(
                        "replace %s on %s".formatted(field.toString(), label),
                        field,
                        condition,
                        innerField ->
                          field(
                            "to_jsonb(({0}))",
                            JSONB.class,
                            select(newValue)
                              .from(tempTableFinal)
                              .where(originalValue.eq(DBUtils.jsonbToString(innerField)))
                          )
                      );
                    } else {
                      return new ReplaceValuePart(
                        "replace %s on %s".formatted(field.toString(), label),
                        field,
                        condition,
                        innerField ->
                          field(
                            select(newValue).from(tempTableFinal).where(originalValue.eq((Field<String>) innerField))
                          )
                      );
                    }
                  }
                )
              )
              .toList()
          );

          if (JobConfigurationProperty.isOn(ctx.settings(), "drop-table")) {
            job.scheduleParts("cleanup", List.of(new DropTablePart("Destroy temp table (staging)", tempTableStaging)));
            job.scheduleParts("cleanup", List.of(new DropTablePart("Destroy temp table (final)", tempTableFinal)));
          }

          return job;
        }
      )
    );
  }
}
