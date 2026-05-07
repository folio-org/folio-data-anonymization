package org.folio.anonymization.jobs;

import static dev.tamboui.toolkit.Toolkit.row;
import static dev.tamboui.toolkit.Toolkit.spacer;
import static dev.tamboui.toolkit.Toolkit.text;

import java.util.List;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Triple;
import org.folio.anonymization.config.JobConfig;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.BatchGenerationFromTablePart;
import org.folio.anonymization.jobs.templates.RedactPart;
import org.folio.anonymization.repository.UtilRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class FreeTextRedaction implements JobFactory {

  private static final List<FieldReference> NOTE_AND_COMMENT_FIELDS = List.of(
    new FieldReference("agreements", "agreement_relationship", "ar_note"),
    new FieldReference("agreements", "document_attachment", "da_note"),
    new FieldReference("agreements", "entitlement", "ent_note"),
    new FieldReference("agreements", "entitlement", "ent_description"),
    new FieldReference("agreements", "license_amendment_status", "las_note"),
    new FieldReference("agreements", "period", "per_note"),
    new FieldReference("agreements", "remote_license_link", "rll_note"),
    new FieldReference("agreements", "subscription_agreement", "sa_license_note"),
    new FieldReference("agreements", "subscription_agreement_org_role", "saor_note"),
    new FieldReference("agreements", "package_content_item", "pci_note"),
    new FieldReference("agreements", "sa_event_history", "eh_notes"),
    new FieldReference("agreements", "usage_data_provider", "udp_note"),
    new FieldReference("circulation_storage", "actual_cost_record", "jsonb", "$.additionalInfoForStaff"),
    new FieldReference(
      "data_export_spring",
      "export_config",
      "export_type_specific_parameters",
      "$.vendorEdiOrdersExportConfig.ediFtp.notes"
    ),
    new FieldReference(
      "data_export_spring",
      "job",
      "export_type_specific_parameters",
      "$.vendorEdiOrdersExportConfig.ediFtp.notes"
    ),
    new FieldReference("entities_links", "authority", "notes", "$[*].note"),
    new FieldReference("entities_links", "authority_archive", "notes", "$[*].note"),
    new FieldReference("erm_usage", "counter_reports", "jsonb", "$.editReason"),
    new FieldReference("erm_usage", "custom_reports", "jsonb", "$.note"),
    new FieldReference("erm_usage", "usage_data_providers", "jsonb", "$.notes"),
    new FieldReference("feesfines", "feefineactions", "jsonb", "$.comments"),
    new FieldReference("inventory_storage", "holdings_record", "jsonb", "$.administrativeNotes[*]"),
    new FieldReference("inventory_storage", "holdings_record", "jsonb", "$.notes[*].note"),
    new FieldReference("inventory_storage", "holdings_record", "jsonb", "$.holdingsStatements[*].note"),
    new FieldReference("inventory_storage", "holdings_record", "jsonb", "$.holdingsStatementsForIndexes[*].note"),
    new FieldReference("inventory_storage", "holdings_record", "jsonb", "$.holdingsStatementsForSupplements[*].note"),
    new FieldReference("inventory_storage", "audit_holdings_record", "jsonb", "$.record.administrativeNotes[*]"),
    new FieldReference("inventory_storage", "audit_holdings_record", "jsonb", "$.record.notes[*].note"),
    new FieldReference("inventory_storage", "audit_holdings_record", "jsonb", "$.record.holdingsStatements[*].note"),
    new FieldReference(
      "inventory_storage",
      "audit_holdings_record",
      "jsonb",
      "$.record.holdingsStatementsForIndexes[*].note"
    ),
    new FieldReference(
      "inventory_storage",
      "audit_holdings_record",
      "jsonb",
      "$.record.holdingsStatementsForSupplements[*].note"
    ),
    new FieldReference("inventory_storage", "instance", "jsonb", "$.notes[*].note"),
    new FieldReference("inventory_storage", "instance", "jsonb", "$.administrativeNotes[*]"),
    new FieldReference("inventory_storage", "audit_instance", "jsonb", "$.record.notes[*].note"),
    new FieldReference("inventory_storage", "audit_instance", "jsonb", "$.record.administrativeNotes[*]"),
    new FieldReference("inventory_storage", "item", "jsonb", "$.notes[*].note"),
    new FieldReference("inventory_storage", "item", "jsonb", "$.administrativeNotes[*]"),
    new FieldReference("inventory_storage", "item", "jsonb", "$.circulationNotes[*].note"),
    new FieldReference("inventory_storage", "audit_item", "jsonb", "$.record.notes[*].note"),
    new FieldReference("inventory_storage", "audit_item", "jsonb", "$.record.administrativeNotes[*]"),
    new FieldReference("inventory_storage", "audit_item", "jsonb", "$.record.circulationNotes[*].note"),
    new FieldReference("invoice_storage", "invoice_lines", "jsonb", "$.comment"),
    new FieldReference("invoice_storage", "invoices", "jsonb", "$.note"),
    new FieldReference("licenses", "document_attachment", "da_note"),
    new FieldReference("licenses", "license_org", "sao_note"),
    new FieldReference("licenses", "license_org_role", "lior_note"),
    new FieldReference("notes", "note", "title"),
    new FieldReference("notes", "note", "content"),
    new FieldReference("notes", "note", "indexed_content"),
    new FieldReference("oa", "charge", "ch_discount_note"),
    new FieldReference("oa", "checklist_item_note", "clin_note"),
    new FieldReference("oa", "payer", "cpy_payer_note"),
    new FieldReference("oa", "publication_request_history", "prh_note"),
    new FieldReference("oa", "publication_status", "ps_status_note"),
    new FieldReference("orders_storage", "pieces", "jsonb", "$.comment"),
    new FieldReference("orders_storage", "po_line", "jsonb", "$.cancellationRestrictionNote"),
    new FieldReference("orders_storage", "po_line", "jsonb", "$.details.receivingNote"),
    new FieldReference("orders_storage", "po_line", "jsonb", "$.renewalNote"),
    new FieldReference("orders_storage", "po_line", "jsonb", "$.vendorDetail.noteFromVendor"),
    new FieldReference("orders_storage", "purchase_order", "jsonb", "$.closeReason.note"),
    new FieldReference("orders_storage", "purchase_order", "jsonb", "$.closeReason.reason"),
    new FieldReference("orders_storage", "purchase_order", "jsonb", "$.notes[*]"),
    new FieldReference("orders_storage", "purchase_order", "jsonb", "$.ongoing.notes"),
    new FieldReference("orders_storage", "routing_list", "jsonb", "$.notes"),
    new FieldReference("orders_storage", "titles", "jsonb", "$.receivingNote"),
    new FieldReference("organizations_storage", "banking_information", "jsonb", "$.notes"),
    new FieldReference("organizations_storage", "contacts", "jsonb", "$.notes"),
    new FieldReference("organizations_storage", "contacts", "jsonb", "$.urls[*].notes"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.contacts.notes"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.contacts.urls[*].notes"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.privilegedContacts.notes"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.privilegedContacts.urls[*].notes"),
    new FieldReference("organizations_storage", "interfaces", "jsonb", "$.notes"),
    new FieldReference("organizations_storage", "interfaces", "jsonb", "$.statisticsNotes"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.urls[*].notes"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.agreements[*].notes"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.edi.notes"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.edi.ediFtp.notes"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.edi.ediJob.schedulingNotes"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.accounts[*].notes"),
    new FieldReference("organizations_storage", "privileged_contacts", "jsonb", "$.notes"),
    new FieldReference("organizations_storage", "privileged_contacts", "jsonb", "$.urls[*].notes"),
    new FieldReference("reading_room", "patron_permission", "notes"),
    new FieldReference("remote_storage", "item_notes", "note"),
    new FieldReference("remote_storage", "retrieval_queue", "request_note"),
    new FieldReference("requests_mediated", "batch_request", "patron_comments"),
    new FieldReference("requests_mediated", "batch_request_split", "patron_comments"),
    new FieldReference("requests_mediated", "mediated_request", "patron_comments"),
    new FieldReference("requests_mediated", "mediated_request", "cancellation_additional_information"),
    new FieldReference("search", "holding", "json", "$.administrativeNotes[*]"),
    new FieldReference("search", "holding", "json", "$.notes[*].note"),
    new FieldReference("search", "holding", "json", "$.holdingsStatements[*].note"),
    new FieldReference("search", "holding", "json", "$.holdingsStatementsForIndexes[*].note"),
    new FieldReference("search", "holding", "json", "$.holdingsStatementsForSupplements[*].note"),
    new FieldReference("search", "instance", "json", "$.notes[*].note"),
    new FieldReference("search", "instance", "json", "$.administrativeNotes[*]"),
    new FieldReference("search", "item", "json", "$.notes[*].note"),
    new FieldReference("search", "item", "json", "$.administrativeNotes[*]"),
    new FieldReference("search", "item", "json", "$.circulationNotes[*].note"),
    new FieldReference("serials_management", "predicted_piece_set", "pps_note"),
    new FieldReference("serials_management", "enumeration_numeric_leveltmrf", "etltmrf_internal_note"),
    new FieldReference("serials_management", "serial_note", "sn_note"),
    new FieldReference("tlr", "ecs_tlr", "patron_comments")
  );

  private static final List<FieldReference> LABELS_AND_DESCRIPTION_FIELDS = List.of(
    new FieldReference("agreements", "document_attachment", "da_name"),
    new FieldReference("agreements", "entitlement", "ent_description"),
    new FieldReference("agreements", "persistent_job", "job_name"),
    new FieldReference("agreements", "refdata_value", "rdv_label"),
    new FieldReference("agreements", "subscription_agreement", "sa_name"),
    new FieldReference("circulation_storage", "cancellation_reason", "jsonb", "$.description"),
    new FieldReference("circulation_storage", "cancellation_reason", "jsonb", "$.publicDescription"),
    new FieldReference("data_export", "job_profiles", "jsonb", "$.description"),
    new FieldReference("data_export", "mapping_profiles", "jsonb", "$.description"),
    new FieldReference("data_import", "default_file_extensions", "jsonb", "$.description"),
    new FieldReference("di_converter_storage", "action_profiles", "jsonb", "$.description"),
    new FieldReference("di_converter_storage", "job_profiles", "jsonb", "$.description"),
    new FieldReference("di_converter_storage", "mapping_profiles", "jsonb", "$.description"),
    new FieldReference("di_converter_storage", "match_profiles", "jsonb", "$.description"),
    new FieldReference("di_converter_storage", "profile_snapshots", "jsonb", "$.content.description"),
    new FieldReference("feesfines", "manual_block_templates", "jsonb", "$.blockTemplate.desc"),
    new FieldReference("feesfines", "manualblocks", "jsonb", "$.desc"),
    new FieldReference("feesfines", "waives", "jsonb", "$.description"),
    new FieldReference("kb_ebsco_java", "access_types", "name"),
    new FieldReference("kb_ebsco_java", "access_types", "description"),
    new FieldReference("kb_ebsco_java", "providers", "name"),
    new FieldReference("kb_ebsco_java", "kb_credentials", "name"),
    new FieldReference("kb_ebsco_java", "resources", "name"),
    new FieldReference("licenses", "document_attachment", "da_name"),
    new FieldReference("lists", "list_details", "name"),
    new FieldReference("lists", "list_details", "description"),
    new FieldReference("lists", "list_versions", "name"),
    new FieldReference("lists", "list_versions", "description"),
    new FieldReference("oa", "address", "add_name"),
    new FieldReference("oa", "charge", "ch_description"),
    new FieldReference("oa", "checklist_item_definition", "clid_label"),
    new FieldReference("oa", "checklist_item_definition", "clid_description"),
    new FieldReference("orders_storage", "routing_list", "jsonb", "$.names"),
    new FieldReference("orders_storage", "prefixes", "jsonb", "$.description"),
    new FieldReference("orders_storage", "prefixes", "jsonb", "$.name"),
    new FieldReference("orders_storage", "suffixes", "jsonb", "$.description"),
    new FieldReference("orders_storage", "suffixes", "jsonb", "$.name"),
    new FieldReference("orders_storage", "export_history", "jsonb", "$.jobName"),
    new FieldReference("orders_storage", "po_line", "jsonb", "$.description"),
    new FieldReference("orders_storage", "po_line", "jsonb", "$.poLineDescription"),
    new FieldReference("organizations_storage", "contacts", "jsonb", "$.emails[*].description"),
    new FieldReference("organizations_storage", "contacts", "jsonb", "$.urls[*].description"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.contacts.emails[*].description"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.contacts.urls[*].description"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.privilegedContacts.emails[*].description"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.privilegedContacts.urls[*].description"),
    new FieldReference("organizations_storage", "interface_credentials", "jsonb", "$.name"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.description"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.aliases[*].description"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.urls[*].description"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.emails[*].description"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.agreements[*].name"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.accounts[*].name"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.accounts[*].description"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.changelogs[*].description"),
    new FieldReference("organizations_storage", "privileged_contacts", "jsonb", "$.emails[*].description"),
    new FieldReference("organizations_storage", "privileged_contacts", "jsonb", "$.urls[*].description"),
    new FieldReference("roles_keycloak", "role", "description"),
    new FieldReference("service_interaction", "refdata_value", "rdv_label"),
    new FieldReference("service_interaction", "dashboard", "dashb_name"),
    new FieldReference("service_interaction", "dashboard", "dashb_description"),
    new FieldReference("service_interaction", "widget_instance", "wins_name"),
    new FieldReference("tags", "tags", "jsonb", "$.description"),
    new FieldReference("template_engine", "template", "jsonb", "$.description")
  );

  private static final FieldReference LISTS_USER_FRIENDLY_QUERY = new FieldReference(
    "lists",
    "list_details",
    "user_friendly_query"
  );

  private static final List<FieldReference> MISC_FELDS = List.of(
    new FieldReference("agreements", "document_attachment", "da_location"),
    new FieldReference("bulk_operations", "bulk_operation", "user_friendly_query"),
    new FieldReference("inn_reach", "transaction_local_hold", "patron_home_library"),
    new FieldReference("licenses", "document_attachment", "da_location"),
    LISTS_USER_FRIENDLY_QUERY,
    new FieldReference("lists", "list_versions", "user_friendly_query"), // removed at the same time, so both will exist or neither
    new FieldReference("oa", "party", "p_institution_level_2"),
    new FieldReference("oa", "correspondence", "prc_content"),
    new FieldReference("oa", "correspondence", "prc_correspondent"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.accounts[*].contactInfo")
  );

  @Autowired
  private SharedExecutionContext context;

  @Autowired
  private UtilRepository utilRepository;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    boolean modListsUserFriendlyQueryExists = utilRepository.doesColumnExist(
      LISTS_USER_FRIENDLY_QUERY,
      tenant.tenant()
    );
    return Stream
      .of(
        Triple.of(
          "Free text redaction — staff notes and comments",
          "Redacts staff notes and patron comments.",
          NOTE_AND_COMMENT_FIELDS
        ),
        Triple.of(
          "Free text redaction — labels and descriptions",
          "Redacts object labels/names and descriptions which may potentially reference PII.",
          LABELS_AND_DESCRIPTION_FIELDS
        ),
        Triple.of(
          "Free text redaction — miscellaneous fields",
          "Redacts free text fields which contain irregular PII not covered by other categories.",
          MISC_FELDS
        )
      )
      .map(t ->
        new JobBuilder(
          t.getLeft(),
          t.getMiddle(),
          tenant,
          context,
          disableUserFriendlyQueryColumnsIfNeeded(
            JobConfigurationProperty.fromFieldList(t.getRight(), tenant),
            modListsUserFriendlyQueryExists
          ),
          ctx ->
            new Job(ctx, List.of("prepare", "redact"))
              .scheduleParts(
                "prepare",
                JobConfigurationProperty
                  .getEnabledFields(ctx.settings())
                  .map(field ->
                    new BatchGenerationFromTablePart<>(
                      "Prepare to redact " + field.toString(),
                      field,
                      JobConfig.BATCH_SIZE,
                      "redact",
                      (label, condition, start, end) ->
                        new RedactPart("Redact " + field.toString() + " on " + label, field, condition)
                    )
                  )
                  .toList()
              )
        )
      )
      .toList();
  }

  private List<JobConfigurationProperty> disableUserFriendlyQueryColumnsIfNeeded(
    List<JobConfigurationProperty> fromFieldList,
    boolean doesColumnExist
  ) {
    if (!doesColumnExist) {
      return fromFieldList
        .stream()
        .map(setting -> {
          if (
            setting.getKey() instanceof FieldReference field &&
            field.schema().equals("lists") &&
            field.column().equals("user_friendly_query")
          ) {
            return setting
              .withDisabled(true)
              .withLabel(
                row(
                  text("mod_"),
                  text(field.toString()).crossedOut(),
                  spacer(1),
                  text("(not available for tenant)").italic()
                )
              );
          }
          return setting;
        })
        .toList();
    }
    return fromFieldList;
  }
}
