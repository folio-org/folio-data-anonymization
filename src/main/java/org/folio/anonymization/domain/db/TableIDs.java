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
    Pair.of(new FieldReference("circulation_storage", "print_events", "id"), UUID.class),
    Pair.of(new FieldReference("consortia_keycloak", "inactive_user_tenant", "id"), UUID.class),
    Pair.of(new FieldReference("consortia_keycloak", "user_tenant", "id"), UUID.class),
    Pair.of(new FieldReference("copycat", "profile", "id"), UUID.class),
    Pair.of(new FieldReference("data_export_spring", "job", "id"), UUID.class),
    Pair.of(new FieldReference("data_export", "job_profiles", "id"), UUID.class),
    Pair.of(new FieldReference("data_export", "mapping_profiles", "id"), UUID.class),
    Pair.of(new FieldReference("data_import", "default_file_extensions", "id"), UUID.class),
    Pair.of(new FieldReference("data_import", "file_extensions", "id"), UUID.class),
    Pair.of(new FieldReference("data_import", "upload_definitions", "id"), UUID.class),
    Pair.of(new FieldReference("di_converter_storage", "action_profiles", "id"), UUID.class),
    Pair.of(new FieldReference("di_converter_storage", "job_profiles", "id"), UUID.class),
    Pair.of(new FieldReference("di_converter_storage", "mapping_profiles", "id"), UUID.class),
    Pair.of(new FieldReference("di_converter_storage", "marc_field_protection_settings", "id"), UUID.class),
    Pair.of(new FieldReference("di_converter_storage", "match_profiles", "id"), UUID.class),
    Pair.of(new FieldReference("di_converter_storage", "profile_snapshots", "id"), UUID.class),
    Pair.of(new FieldReference("erm_usage", "usage_data_providers", "id"), UUID.class),
    Pair.of(new FieldReference("inn_reach", "agency_location_ac_mapping", "id"), UUID.class),
    Pair.of(new FieldReference("inn_reach", "agency_location_location_mapping", "id"), UUID.class),
    Pair.of(new FieldReference("inn_reach", "agency_location_lsc_mapping", "id"), UUID.class),
    Pair.of(new FieldReference("inn_reach", "agency_location_mapping", "id"), UUID.class),
    Pair.of(new FieldReference("inn_reach", "central_patron_type_mapping", "id"), UUID.class),
    Pair.of(new FieldReference("inn_reach", "central_server", "id"), UUID.class),
    Pair.of(new FieldReference("inn_reach", "contribution_criteria_configuration", "id"), UUID.class),
    Pair.of(new FieldReference("inn_reach", "contribution", "id"), UUID.class),
    Pair.of(new FieldReference("inn_reach", "inn_reach_location", "id"), UUID.class),
    Pair.of(new FieldReference("inn_reach", "inn_reach_recall_user", "id"), UUID.class),
    Pair.of(new FieldReference("inn_reach", "inn_reach_transaction", "id"), UUID.class),
    Pair.of(new FieldReference("inn_reach", "item_contribution_options_configuration", "id"), UUID.class),
    Pair.of(new FieldReference("inn_reach", "item_type_mapping", "id"), UUID.class),
    Pair.of(new FieldReference("inn_reach", "job_execution_status", "id"), UUID.class),
    Pair.of(new FieldReference("inn_reach", "library_mapping", "id"), UUID.class),
    Pair.of(new FieldReference("inn_reach", "location_mapping", "id"), UUID.class),
    Pair.of(new FieldReference("inn_reach", "marc_field_configuration", "id"), UUID.class),
    Pair.of(new FieldReference("inn_reach", "marc_transformation_options_settings", "id"), UUID.class),
    Pair.of(new FieldReference("inn_reach", "material_type_mapping", "id"), UUID.class),
    Pair.of(new FieldReference("inn_reach", "ongoing_contribution_status", "id"), UUID.class),
    Pair.of(new FieldReference("inn_reach", "paging_slip_template", "id"), UUID.class),
    Pair.of(new FieldReference("inn_reach", "patron_type_mapping", "id"), UUID.class),
    Pair.of(new FieldReference("inn_reach", "transaction_hold", "id"), UUID.class),
    Pair.of(new FieldReference("inn_reach", "transaction_pickup_location", "id"), UUID.class),
    Pair.of(new FieldReference("inn_reach", "user_custom_field_mapping", "id"), UUID.class),
    Pair.of(new FieldReference("inn_reach", "visible_patron_field_config", "id"), UUID.class),
    Pair.of(new FieldReference("invoice_storage", "batch_vouchers", "id"), UUID.class),
    Pair.of(new FieldReference("kb_ebsco_java", "kb_credentials", "id"), UUID.class),
    Pair.of(new FieldReference("kb_ebsco_java", "usage_consolidation_settings", "id"), UUID.class),
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
    Pair.of(new FieldReference("password_validator", "validationrules", "id"), UUID.class),
    Pair.of(new FieldReference("remote_storage", "remote_storage_configurations", "id"), UUID.class),
    Pair.of(new FieldReference("remote_storage", "retrieval_queue", "id"), UUID.class),
    Pair.of(new FieldReference("requests_mediated", "batch_request_split", "id"), UUID.class),
    Pair.of(new FieldReference("requests_mediated", "batch_request", "id"), UUID.class),
    Pair.of(new FieldReference("requests_mediated", "mediated_request", "id"), UUID.class),
    Pair.of(new FieldReference("source_record_manager", "job_execution", "id"), UUID.class),
    Pair.of(new FieldReference("users", "addresstype", "id"), UUID.class),
    Pair.of(new FieldReference("users", "configuration", "id"), UUID.class),
    Pair.of(new FieldReference("users", "custom_fields", "id"), UUID.class),
    Pair.of(new FieldReference("users", "departments", "id"), UUID.class),
    Pair.of(new FieldReference("users", "groups", "id"), UUID.class),
    Pair.of(new FieldReference("users", "proxyfor", "id"), UUID.class),
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
