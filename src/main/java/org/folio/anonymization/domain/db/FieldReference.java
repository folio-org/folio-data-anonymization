package org.folio.anonymization.domain.db;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.commons.text.StringSubstitutor;
import org.folio.anonymization.domain.folio.Tenant;
import org.folio.anonymization.util.DBUtils;
import org.folio.anonymization.util.ListUtils;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Table;
import org.jooq.impl.DSL;

/**
 * Defines a field's location (without the tenant_mod_ prefix). For example:
 *
 * @example
 * new FieldReference("users", "users", "id", null);
 * new FieldReference("users", "users", "jsonb", "$.id");
 */
public record FieldReference(String schema, String table, String column, String jsonPath) {
  public FieldReference(String schema, String table, String column) {
    this(schema, table, column, null);
  }

  public String toString() {
    if (jsonPath == null) {
      return String.format("%s.%s.%s", schema, table, column);
    } else {
      return String.format("%s.%s.%s->'%s'", schema, table, column, jsonPath);
    }
  }

  public Table<?> table(Tenant tenant) {
    return DSL.table(DSL.name(DBUtils.getSchemaName(tenant.id(), schema), table));
  }

  public TableReference tableReference() {
    return new TableReference(this.schema, this.table);
  }

  public Field<Object> baseColumn(Tenant tenant) {
    return DSL.field(DSL.name(DBUtils.getSchemaName(tenant.id(), schema), table, column));
  }

  public <T> Field<T> baseColumn(Tenant tenant, Class<T> clazz) {
    return DSL.field(DSL.name(DBUtils.getSchemaName(tenant.id(), schema), table, column), clazz);
  }

  public <T> Field<T> field(Tenant tenant, Class<T> clazz) {
    if (jsonPath == null) {
      return this.baseColumn(tenant, clazz);
    } else {
      return DSL.field(
        "jsonb_path_query({0}, {1}) #>> '{}'",
        clazz,
        baseColumn(tenant, JSONB.class),
        DSL.inline(jsonPath)
      );
    }
  }

  /**
   * Returns a jsonb_set value to replace this field with a given value.
   * The provided replacement will be passed a variable referring to the Field<JSONB>
   * represented by the fully-resolved version of this FieldReference.
   */
  public Field<JSONB> jsonbSet(Tenant tenant, Function<Field<JSONB>, Field<JSONB>> replacement) {
    if (jsonPath == null) {
      throw new UnsupportedOperationException("Cannot use jsonb_set for a non-JSONB field");
    }

    Field<JSONB> parentColumn = baseColumn(tenant, JSONB.class);
    // splits nested arrays into separate groups, e.g. $.foo.bar[*].baz becomes [[foo,bar],[baz]]
    List<List<String>> parts = Arrays
      .stream(jsonPath.substring(2).split("\\[\\*\\]\\."))
      .map(part -> part.split("\\."))
      .map(Arrays::asList)
      .toList();

    List<Object> bindings = new ArrayList<>();
    bindings.add(parentColumn);
    bindings.add(parts.get(0).toArray(new String[0]));
    return DSL.field(
      "jsonb_set_lax({0}, {1}, %s, false, 'return_target')".formatted(
          getReplacement(parts.subList(1, parts.size()), parentColumn, parts.get(0), replacement, bindings)
        ),
      JSONB.class,
      bindings.toArray()
    );
  }

  /**
   * Get the jsonb_agg for a nested jsonb_set recursively
   */
  protected String getReplacement(
    List<List<String>> remainingParts,
    Field<JSONB> parentColumn,
    List<String> parentPath,
    Function<Field<JSONB>, Field<JSONB>> replacement,
    List<Object> bindings
  ) {
    if (remainingParts.isEmpty()) {
      return (
        "{" +
        ListUtils.addAndGetIndex(
          bindings,
          replacement.apply(DBUtils.resolveFieldProperties(parentColumn, parentPath))
        ) +
        "}"
      );
    }
    int parentIndex = ListUtils.addAndGetIndex(bindings, DBUtils.resolveFieldProperties(parentColumn, parentPath));
    int parentAsTextIndex = ListUtils.addAndGetIndex(
      bindings,
      DBUtils.resolveFieldPropertiesToString(parentColumn, parentPath)
    );
    Field<JSONB> innerElement = DSL.field("elem" + parentAsTextIndex, JSONB.class);
    int innerElementIndex = ListUtils.addAndGetIndex(bindings, innerElement);
    int innerElementPropertyAsTextIndex = ListUtils.addAndGetIndex(
      bindings,
      DBUtils.resolveFieldPropertiesToString(innerElement, remainingParts.get(0))
    );
    int innerElementPropertyPathIndex = ListUtils.addAndGetIndex(
      bindings,
      remainingParts.get(0).toArray(new String[0])
    );
    String innerReplacement = getReplacement(
      remainingParts.subList(1, remainingParts.size()),
      innerElement,
      remainingParts.get(0),
      replacement,
      bindings
    );
    return new StringSubstitutor(
      Map.ofEntries(
        Map.entry("parent", parentIndex),
        Map.entry("parentAsText", parentAsTextIndex),
        Map.entry("innerElement", innerElementIndex),
        Map.entry("innerElementPropertyAsText", innerElementPropertyAsTextIndex),
        Map.entry("innerElementPropertyPath", innerElementPropertyPathIndex),
        Map.entry("replacementSqlValue", innerReplacement)
      )
    )
      .replace(
        """
        case
          when {${parentAsText}} is null then null
          else (
            select jsonb_agg(
              case
                when {${innerElementPropertyAsText}} is null then {${innerElement}}
                else jsonb_set_lax(
                  {${innerElement}},
                  {${innerElementPropertyPath}},
                  ${replacementSqlValue},
                  false,
                  'return_target'
                )
              end
            ) from jsonb_array_elements({${parent}}) as {${innerElement}}
          )
        end
        """
      );
  }
}
