package org.folio.anonymization.domain.db;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.Pair;
import org.folio.anonymization.domain.folio.Tenant;
import org.folio.anonymization.jobs.templates.BatchGenerationFromTablePart;
import org.jooq.Field;

/**
 * Reference of table ID columns and their types, to be used where necessary
 * (such as {@link BatchGenerationFromTablePart})
 */
@UtilityClass
public class TableIDs {

  private static final List<Pair<FieldReference, Class<?>>> TABLE_ID_FIELD_LIST = List.of(
    Pair.of(new FieldReference("agreements", "alternate_name", "an_id"), String.class),
    Pair.of(new FieldReference("agreements", "kbart_import_job", "id"), String.class),
    Pair.of(new FieldReference("agreements", "org", "org_id"), String.class),
    Pair.of(new FieldReference("agreements", "package", "id"), String.class),
    Pair.of(new FieldReference("batch_print", "printing", "id"), UUID.class),
    Pair.of(new FieldReference("copycat", "profile", "id"), UUID.class),
    Pair.of(new FieldReference("erm_usage", "usage_data_providers", "id"), UUID.class),
    Pair.of(new FieldReference("invoice_storage", "batch_vouchers", "id"), UUID.class),
    Pair.of(new FieldReference("licenses", "alternate_name", "an_id"), String.class),
    Pair.of(new FieldReference("licenses", "org", "org_id"), String.class),
    Pair.of(new FieldReference("oa", "address", "add_id"), String.class),
    Pair.of(new FieldReference("oa", "alternate_email_address", "aea_id"), String.class),
    Pair.of(new FieldReference("oa", "party", "p_id"), String.class),
    Pair.of(new FieldReference("oai_pmh", "configuration_settings", "id"), UUID.class),
    Pair.of(new FieldReference("orders_storage", "export_history", "id"), UUID.class),
    Pair.of(new FieldReference("organizations_storage", "contacts", "id"), UUID.class),
    Pair.of(new FieldReference("organizations_storage", "organizations", "id"), UUID.class),
    Pair.of(new FieldReference("organizations_storage", "privileged_contacts", "id"), UUID.class),
    Pair.of(new FieldReference("users", "staging_users", "id"), UUID.class),
    Pair.of(new FieldReference("users", "user_tenant", "id"), UUID.class),
    Pair.of(new FieldReference("users", "users", "id"), UUID.class)
  );

  private static final Map<TableReference, Pair<FieldReference, Class<?>>> TABLE_ID_MAP;

  static {
    TABLE_ID_MAP =
      TABLE_ID_FIELD_LIST
        .stream()
        .collect(Collectors.toMap(pair -> pair.getLeft().tableReference(), Function.identity()));
  }

  @Nonnull
  public static Pair<FieldReference, Class<?>> getIdFor(TableReference table) {
    if (!TABLE_ID_MAP.containsKey(table)) {
      throw new IllegalArgumentException("No ID found for table: " + table);
    }
    return TABLE_ID_MAP.get(table);
  }

  @Nonnull
  public static Pair<FieldReference, Class<?>> getIdFor(FieldReference field) {
    return getIdFor(field.tableReference());
  }

  @Nonnull
  public static Field<?> getIdFor(TableReference table, Tenant tenant) {
    Pair<FieldReference, Class<?>> idField = getIdFor(table);
    return idField.getLeft().field(tenant, idField.getRight());
  }

  @Nonnull
  public static Field<?> getIdFor(FieldReference field, Tenant tenant) {
    return getIdFor(field.tableReference(), tenant);
  }
}
