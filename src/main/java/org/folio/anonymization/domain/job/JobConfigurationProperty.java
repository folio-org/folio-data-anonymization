package org.folio.anonymization.domain.job;

import static dev.tamboui.toolkit.Toolkit.row;
import static dev.tamboui.toolkit.Toolkit.spacer;
import static dev.tamboui.toolkit.Toolkit.text;

import dev.tamboui.toolkit.element.StyledElement;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.Data;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.db.ModuleTable;
import org.folio.anonymization.domain.db.TableReference;
import org.folio.anonymization.util.NumberUtils;

@Data
public class JobConfigurationProperty {

  private final Object key;

  private final StyledElement<?> label;

  @Nullable
  private Boolean booleanValue;

  private final boolean disabled;

  public JobConfigurationProperty(String label) {
    this.key = label;
    this.label = text(label);
    this.booleanValue = true;
    this.disabled = false;
  }

  public JobConfigurationProperty(String key, String label) {
    this.key = key;
    this.label = text(label);
    this.booleanValue = true;
    this.disabled = false;
  }

  public JobConfigurationProperty(Object key, StyledElement<?> label, boolean defaultValue, boolean disabled) {
    this.key = key;
    this.label = label;
    this.booleanValue = defaultValue;
    this.disabled = disabled;
  }

  public boolean isOn() {
    return !this.disabled && Boolean.TRUE.equals(this.booleanValue);
  }

  public static List<JobConfigurationProperty> fromFieldList(
    List<FieldReference> fields,
    TenantExecutionContext tenantInfo
  ) {
    return fields
      .stream()
      .map(field -> {
        Optional<ModuleTable> foundTable = tenantInfo
          .availableTables()
          .stream()
          .filter(table -> field.schema().equals(table.schema()) && field.table().equals(table.table()))
          .findAny();

        if (foundTable.isEmpty()) {
          return new JobConfigurationProperty(
            field,
            row(
              text("mod_"),
              text(field.toString()).crossedOut(),
              spacer(1),
              text("(not available for tenant)").italic()
            ),
            true,
            true
          );
        } else {
          return new JobConfigurationProperty(
            field,
            row(
              text("mod_"),
              text(field.toString()),
              spacer(1),
              text(String.format("(%s rows)", NumberUtils.abbreviate(foundTable.get().size()))).italic()
            ),
            true,
            false
          );
        }
      })
      .toList();
  }

  public static List<JobConfigurationProperty> fromTableList(
    List<TableReference> tables,
    TenantExecutionContext tenantInfo
  ) {
    return tables
      .stream()
      .map(table -> {
        Optional<ModuleTable> foundTable = tenantInfo
          .availableTables()
          .stream()
          .filter(t -> table.schema().equals(t.schema()) && table.table().equals(t.table()))
          .findAny();

        if (foundTable.isEmpty()) {
          return new JobConfigurationProperty(
            table,
            row(
              text("mod_"),
              text(table.toString()).crossedOut(),
              spacer(1),
              text("(not available for tenant)").italic()
            ),
            true,
            true
          );
        } else {
          return new JobConfigurationProperty(
            table,
            row(
              text("mod_"),
              text(table.toString()),
              spacer(1),
              text(String.format("(%s rows)", NumberUtils.abbreviate(foundTable.get().size()))).italic()
            ),
            true,
            false
          );
        }
      })
      .toList();
  }

  public static Stream<FieldReference> getEnabledFields(List<JobConfigurationProperty> config) {
    return getEnabled(config, FieldReference.class);
  }

  public static Stream<TableReference> getEnabledTables(List<JobConfigurationProperty> config) {
    return getEnabled(config, TableReference.class);
  }

  public static <T> Stream<T> getEnabled(List<JobConfigurationProperty> config, Class<T> clazz) {
    return config
      .stream()
      .filter(JobConfigurationProperty::isOn)
      .map(JobConfigurationProperty::getKey)
      .filter(clazz::isInstance)
      .map(clazz::cast);
  }

  public static boolean isOn(List<JobConfigurationProperty> haystack, Object key) {
    return haystack
      .stream()
      .filter(JobConfigurationProperty::isOn)
      .map(JobConfigurationProperty::getKey)
      .anyMatch(key::equals);
  }
}
