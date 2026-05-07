package org.folio.anonymization.jobs;

import static dev.tamboui.toolkit.Toolkit.row;
import static dev.tamboui.toolkit.Toolkit.spacer;
import static dev.tamboui.toolkit.Toolkit.text;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.selectDistinct;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.config.JobConfig;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.db.ModuleTable;
import org.folio.anonymization.domain.db.TableReference;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.JobPart;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.BatchGenerationFromEachRowPart;
import org.folio.anonymization.jobs.templates.BatchGenerationFromTablePart;
import org.folio.anonymization.jobs.templates.RedactPart;
import org.folio.anonymization.jobs.templates.ReplaceOIDPart;
import org.folio.anonymization.jobs.templates.ReplaceValueFromListPart;
import org.folio.anonymization.jobs.templates.ReplaceValuePart;
import org.folio.anonymization.util.NumberUtils;
import org.folio.anonymization.util.RandomValueUtils;
import org.jooq.Condition;
import org.jooq.Field;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class GroovyStyleCustomFieldAnonymization implements JobFactory {

  private static final List<String> GROOVY_MODULES = List.of("agreements", "licenses", "service_interaction");
  private static final List<FieldReference> FIELDS = List.of(
    new FieldReference("agreements", "custom_property_blob", "value"),
    new FieldReference("agreements", "custom_property_integer", "value"),
    new FieldReference("agreements", "custom_property_multi_integer_value", "value_big_integer"),
    new FieldReference("agreements", "custom_property_multi_text_value", "value_string"),
    new FieldReference("agreements", "custom_property_text", "value"),
    new FieldReference("licenses", "custom_property_blob", "value"),
    new FieldReference("licenses", "custom_property_integer", "value"),
    new FieldReference("licenses", "custom_property_multi_integer_value", "value_big_integer"),
    new FieldReference("licenses", "custom_property_multi_text_value", "value_string"),
    new FieldReference("licenses", "custom_property_text", "value"),
    new FieldReference("service_interaction", "custom_property_blob", "value"),
    new FieldReference("service_interaction", "custom_property_integer", "value"),
    new FieldReference("service_interaction", "custom_property_multi_integer_value", "value_big_integer"),
    new FieldReference("service_interaction", "custom_property_multi_text_value", "value_string"),
    new FieldReference("service_interaction", "custom_property_text", "value")
  );

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "Custom property definitions (not recommended)",
        "*SEE BELOW FOR LIMITATIONS* Replaces the names and descriptions of custom fields with random values.",
        tenant,
        context,
        GROOVY_MODULES
          .stream()
          .map(module -> {
            Optional<ModuleTable> table = tenant
              .availableTables()
              .stream()
              .filter(t -> module.equals(t.schema()) && "custom_property_definition".equals(t.table()))
              .findAny();

            if (table.isEmpty()) {
              return new JobConfigurationProperty(
                module,
                row(
                  text("mod_" + module + " custom property definitions").crossedOut(),
                  spacer(1),
                  text("(not available for tenant)").italic()
                ),
                true,
                false
              );
            } else {
              return new JobConfigurationProperty(
                module,
                row(
                  text("mod_" + module + " custom property definitions"),
                  spacer(1),
                  text(String.format("(%s rows)", NumberUtils.abbreviate(table.get().size()))).italic()
                ),
                false,
                false
              );
            }
          })
          .toList(),
        ctx -> {
          Job job = new Job(ctx, List.of("prepare", "overwrite"));

          job.scheduleParts(
            "prepare",
            ctx
              .settings()
              .stream()
              .filter(JobConfigurationProperty::isOn)
              .map(JobConfigurationProperty::getKey)
              .filter(String.class::isInstance)
              .map(String.class::cast)
              .map(module -> {
                TableReference table = new TableReference(module, "custom_property_definition");
                return new BatchGenerationFromTablePart<>(
                  "Prepare to overwrite custom field definitions in " + table.toString(),
                  table,
                  JobConfig.BATCH_SIZE,
                  "overwrite",
                  (label, condition, start, end) ->
                    new ReplaceValueFromListPart(
                      "Replace custom field definitions in " + table.schema() + " on " + label,
                      List.of(table.field("pd_name"), table.field("pd_label"), table.field("pd_description")),
                      condition,
                      RandomValueUtils.groovyCustomFieldDefinitions(start, end),
                      List.of(
                        field("new_name", String.class),
                        field("new_label", String.class),
                        field("new_description", String.class)
                      )
                    )
                );
              })
              .toList()
          );

          return job;
        }
      ),
      new JobBuilder(
        "Custom property values",
        "Replaces text and integer values of custom fields with random values.",
        tenant,
        context,
        Stream
          .concat(
            GROOVY_MODULES
              .stream()
              .map(module -> {
                Optional<ModuleTable> table = tenant
                  .availableTables()
                  .stream()
                  .filter(t -> module.equals(t.schema()) && "custom_property_definition".equals(t.table()))
                  .findAny();

                if (table.isEmpty()) {
                  return new JobConfigurationProperty(
                    module,
                    row(
                      text("mod_" + module + " custom property pick lists").crossedOut(),
                      spacer(1),
                      text("(not available for tenant)").italic()
                    ),
                    true,
                    true
                  );
                } else {
                  return new JobConfigurationProperty(
                    module,
                    row(text("mod_" + module + " custom property pick lists")),
                    true,
                    false
                  );
                }
              }),
            JobConfigurationProperty.fromFieldList(FIELDS, tenant).stream()
          )
          .toList(),
        ctx -> {
          Job job = new Job(
            ctx,
            List.of("prepare", "overwrite", "prepare-picklist-values", "overwrite-picklist-values")
          );

          job.scheduleParts(
            "prepare",
            ctx
              .settings()
              .stream()
              .filter(JobConfigurationProperty::isOn)
              .map(JobConfigurationProperty::getKey)
              .filter(String.class::isInstance)
              .map(String.class::cast)
              .flatMap(module -> {
                TableReference refdataDefinitions = new TableReference(module, "custom_property_refdata_definition");
                TableReference refdataCategories = new TableReference(module, "refdata_category");
                TableReference refdataValues = new TableReference(module, "refdata_value");

                FieldReference refdataCategoryId = refdataCategories.field("rdc_id");
                FieldReference refdataDefinitionCategoryId = refdataDefinitions.field("category_id");
                FieldReference isInternal = refdataCategories.field("internal");

                Field<String> refdataIdField = refdataCategoryId.baseColumn(tenant.tenant(), String.class);

                return Stream.of(
                  new BatchGenerationFromEachRowPart<>(
                    "Prepare to overwrite pick lists from " + module,
                    selectDistinct(refdataIdField)
                      .from(refdataDefinitions.table(tenant.tenant()))
                      .join(refdataCategories.table(tenant.tenant()))
                      .on(
                        refdataDefinitionCategoryId
                          .baseColumn(tenant.tenant())
                          .eq(refdataCategoryId.baseColumn(tenant.tenant()))
                      )
                      .where(isInternal.baseColumn(tenant.tenant(), Boolean.class).eq(false)),
                    (r, i) -> {
                      String refdataCategoryIdValue = r.get(refdataIdField);
                      job
                        .scheduleParts(
                          "overwrite",
                          List.of(
                            new ReplaceValuePart(
                              "Replace pick list name in " + module + " for ID " + refdataCategoryIdValue,
                              refdataCategories.field("rdc_description"),
                              refdataIdField.eq(refdataCategoryIdValue),
                              field("'{0}'", String.class, RandomValueUtils.pickListName())
                            )
                          )
                        )
                        // this is insanity. picklists will almost certainly never have more than a few values, so one batch is fine.
                        // however, we must know the quantity to generate what we can guarantee is a unique value for each,
                        // and the easiest way to do that is wrap it with batch generation :(
                        .scheduleParts(
                          "prepare-picklist-values",
                          List.of(
                            new BatchGenerationFromTablePart<>(
                              "Prepare to replace pick list values in " +
                              module +
                              " for picklist ID " +
                              refdataCategoryIdValue,
                              refdataValues.field("rdv_id"),
                              String.class,
                              JobConfig.BATCH_SIZE,
                              "overwrite-picklist-values",
                              (label, condition, start, end) ->
                                new ReplaceValueFromListPart(
                                  "Replace pick list value in " +
                                  module +
                                  " for picklist ID " +
                                  refdataCategoryIdValue +
                                  " on " +
                                  label,
                                  List.of(refdataValues.field("rdv_value"), refdataValues.field("rdv_label")),
                                  condition.and(
                                    refdataValues
                                      .field("rdv_owner")
                                      .baseColumn(tenant.tenant(), String.class)
                                      .eq(refdataCategoryIdValue)
                                  ),
                                  RandomValueUtils.pickListValues(i, start, end),
                                  List.of(field("new_value", String.class), field("new_label", String.class))
                                ),
                              refdataValues
                                .field("rdv_owner")
                                .baseColumn(tenant.tenant(), String.class)
                                .eq(refdataCategoryIdValue)
                            )
                          )
                        );
                    }
                  )
                );
              })
              .toList()
          );

          job.scheduleParts(
            "prepare",
            JobConfigurationProperty
              .getEnabledFields(ctx.settings())
              .map(field ->
                new BatchGenerationFromTablePart<>(
                  "Prepare to overwrite " + field.toString(),
                  field,
                  JobConfig.BATCH_SIZE,
                  "overwrite",
                  (label, condition, start, end) -> getOverwritePart(field, label, condition, start, end)
                )
              )
              .toList()
          );

          return job;
        }
      )
    );
  }

  private JobPart getOverwritePart(FieldReference field, String rangeLabel, Condition condition, int start, int end) {
    return switch (field.table()) {
      case "custom_property_blob" -> new ReplaceOIDPart(
        "Replace " + field.toString() + " PG large objects on " + rangeLabel,
        field,
        condition,
        "Replaced during anonymization".getBytes()
      );
      case "custom_property_multi_text_value", "custom_property_text" -> new RedactPart(
        "Redact " + field.toString() + " on " + rangeLabel,
        field,
        condition
      );
      case "custom_property_integer", "custom_property_multi_integer_value" -> new ReplaceValuePart(
        "Replace " + field.toString() + " with random integer [0,1mil) on " + rangeLabel,
        field,
        condition,
        field("floor(random() * 1000000)::int", Integer.class)
      );
      default -> throw new IllegalArgumentException("No overwrite strategy defined for field " + field);
    };
  }
}
