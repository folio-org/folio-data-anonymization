package org.folio.anonymization.jobs;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.primaryKey;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.DSL.unique;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.JobPart;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.BatchGenerationFromSequencePart;
import org.folio.anonymization.jobs.templates.BatchGenerationFromTablePart;
import org.folio.anonymization.jobs.templates.CreateTablePart;
import org.folio.anonymization.jobs.templates.DropTablePart;
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
public class VendorNamesAndCodeAnonymization implements JobFactory {

  private static final int BATCH_SIZE = 2_000;

  private static final List<FieldReference> FIELDS = List.of(
    new FieldReference("agreements", "alternate_name", "an_name"),
    new FieldReference("agreements", "kbart_import_job", "package_source"),
    new FieldReference("agreements", "kbart_import_job", "package_provider"),
    new FieldReference("agreements", "org", "org_name"),
    new FieldReference("agreements", "package", "pkg_source"),
    new FieldReference("copycat", "profile", "jsonb", "$.name"),
    new FieldReference("erm_usage", "usage_data_providers", "jsonb", "$.harvestingConfig.aggregator.name"),
    new FieldReference("erm_usage", "usage_data_providers", "jsonb", "$.harvestingConfig.aggregator.vendorCode"),
    new FieldReference("invoice_storage", "batch_vouchers", "jsonb", "$.batchedVouchers[*].vendorName"),
    new FieldReference("licenses", "alternate_name", "an_name"),
    new FieldReference("licenses", "org", "org_name"),
    new FieldReference("orders_storage", "export_history", "jsonb", "$.vendorName"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.name"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.code"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.aliases[*].value")
  );

  // used to correlate some names with some codes. Allows us to later update the names
  // to be semantically related (e.g. anonymized code ABCD would be able to refer to Org ABCD, LLC or something)
  private static final Map<FieldReference, FieldReference> pairedFields = Map.of(
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.name"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.code")
  );

  @Autowired
  private SharedExecutionContext context;

  @SuppressWarnings("unchecked")
  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "Vendor names/code anonymization",
        "Replaces vendor names and codes with unique anonymized values, ensuring integrity between tables.",
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
          // final contains the full [original,reference,newValue]
          // staging contains only [original,reference]
          // this allows us to copy from one to the other as we generate unique values
          Table<?> tempTableFinal = table(
            name("public", "_danon_" + ctx.tenant().tenant().id() + "_vendor_names_and_codes")
          );
          Table<?> tempTableStaging = table(
            name("public", "_danon_" + ctx.tenant().tenant().id() + "_vendor_names_and_codes_staging")
          );

          Field<String> originalValue = field("original_value", SQLDataType.VARCHAR.notNull());
          // if this row represents a name, its code will be here
          Field<String> referenceCode = field("reference_code", SQLDataType.VARCHAR.null_());
          Field<String> newValue = field("new_value", SQLDataType.VARCHAR.null_());

          Job job = new Job(
            ctx,
            List.of(
              "prepare",
              "enumerate-prep",
              // must be done discretely and as its own part since first inserts take priority
              // over later ones
              "enumerate-correlated",
              "enumerate-independent",
              // single job to count and spawn child generate-new-values jobs
              "generate-new-values-prep",
              "generate-new-values",
              // overwrite generated new values for names that have codes
              "correlate-new-values",
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
                  List.of(originalValue, referenceCode),
                  List.of(primaryKey(originalValue)),
                  true
                ),
                new CreateTablePart(
                  "Create temporary table (final)",
                  tempTableFinal,
                  List.of(originalValue, referenceCode, newValue),
                  List.of(primaryKey(originalValue), unique(newValue)),
                  false
                )
              )
            );
            job.scheduleParts(
              "enumerate-prep",
              Stream
                .concat(
                  JobConfigurationProperty
                    .getEnabledFields(ctx.settings())
                    .filter(pairedFields::containsKey)
                    .map(field -> {
                      FieldReference pairedField = pairedFields.get(field);

                      return new BatchGenerationFromTablePart<>(
                        "Make batches to enumerate correlated data from " + field.toString(),
                        field,
                        BATCH_SIZE,
                        "enumerate-correlated",
                        (label, condition, start, end) ->
                          new InsertIntoTablePart(
                            "Enumerate correlated data from " + field.toString() + " " + label,
                            tempTableStaging,
                            select(field("a"), field("b"))
                              .from(
                                select(
                                  field.field(ctx.tenant().tenant(), String.class).as("a"),
                                  pairedField.field(ctx.tenant().tenant(), String.class).as("b")
                                )
                                  .from(field.table(ctx.tenant().tenant()))
                                  .where(condition)
                              )
                              .where(field("a").isNotNull())
                          )
                      );
                    }),
                  JobConfigurationProperty
                    .getEnabledFields(ctx.settings())
                    .filter(f -> !pairedFields.containsKey(f))
                    .map(field -> {
                      return new BatchGenerationFromTablePart<>(
                        "Make batches to enumerate independent data from " + field.toString(),
                        field,
                        BATCH_SIZE,
                        "enumerate-independent",
                        (label, condition, start, end) ->
                          new InsertIntoTablePart(
                            "Enumerate independent data from " + field.toString() + " " + label,
                            tempTableStaging,
                            select(field("a"))
                              .from(
                                select(field.field(ctx.tenant().tenant(), String.class).as("a"))
                                  .from(field.table(ctx.tenant().tenant()))
                                  .where(condition)
                              )
                              .where(field("a").isNotNull())
                          )
                      );
                    })
                )
                .toList()
            );

            job.scheduleParts(
              "generate-new-values-prep",
              List.of(
                new BatchGenerationFromSequencePart(
                  "Analyze table size for split processing",
                  tempTableStaging,
                  BATCH_SIZE,
                  "generate-new-values",
                  (label, cond, start, end) ->
                    new GenerateValuesPart(
                      "Generate values %s".formatted(label),
                      tempTableFinal,
                      newValue,
                      select(originalValue, referenceCode).from(tempTableStaging).where(cond),
                      RandomValueUtils.codeLikeValueGenerator(start)
                    )
                )
              )
            );
            job.scheduleParts(
              "correlate-new-values",
              List.of(new CorrelateNamesAndCodesPart(tempTableFinal, originalValue, referenceCode, newValue))
            );
          }

          job.scheduleParts(
            "apply-new-values-prep",
            JobConfigurationProperty
              .getEnabledFields(ctx.settings())
              .map(field ->
                new BatchGenerationFromTablePart<>(
                  "Prep to apply new values to " + field.toString(),
                  field,
                  BATCH_SIZE,
                  "apply-new-values",
                  (label, condition, start, end) -> {
                    if (field.jsonPath() != null) {
                      return new ReplaceJSONBValuePart(
                        "replace %s on %s".formatted(field.toString(), label),
                        field,
                        innerField ->
                          field(
                            "to_jsonb(({0}))",
                            JSONB.class,
                            select(newValue)
                              .from(tempTableFinal)
                              .where(originalValue.eq(DBUtils.jsonbToString(innerField)))
                          ),
                        condition
                      );
                    } else {
                      return new ReplaceValuePart(
                        "replace %s on %s".formatted(field.toString(), label),
                        field,
                        innerField ->
                          field(
                            select(newValue).from(tempTableFinal).where(originalValue.eq((Field<String>) innerField))
                          ),
                        condition
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

  private static class CorrelateNamesAndCodesPart extends JobPart {

    private final Table<?> table;

    private final Field<String> key;
    private final Field<String> reference;
    private final Field<String> newValue;

    public CorrelateNamesAndCodesPart(
      Table<?> table,
      Field<String> key,
      Field<String> reference,
      Field<String> newValue
    ) {
      super("Correlate names and codes");
      this.table = table;
      this.key = key;
      this.reference = reference;
      this.newValue = newValue;
    }

    @Override
    protected void execute() {
      this.create()
        .update(table.as("r"))
        .set(
          newValue,
          field(
            "concat(%s, {0}, %s)".formatted(
                RandomValueUtils.randomArrayEntrySql("", "Corp ", "Company ", "The Best "),
                RandomValueUtils.randomArrayEntrySql(" Org", " & Family", " & Co.", ", LLC", ", Inc.", ".com")
              ),
            String.class,
            field(name("c", newValue.getName()), String.class)
          )
        )
        .from(table.as("c"))
        .where(field(name("c", key.getName())).eq(field(name("r", reference.getName()))))
        .execute();
    }
  }
}
