package org.folio.anonymization.jobs;

import java.util.List;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.BatchGenerationFromTablePart;
import org.folio.anonymization.jobs.templates.ReplaceValueFromListPart;
import org.folio.anonymization.util.RandomValueUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AddressAnonymization implements JobFactory {

  private static final List<FieldReference> ADDRESS_FIELDS = List.of(
    new FieldReference("invoice_storage", "batch_vouchers", "jsonb", "$.batchedVouchers[*].vendorAddress.addressLine1"),
    new FieldReference("invoice_storage", "batch_vouchers", "jsonb", "$.batchedVouchers[*].vendorAddress.addressLine2"),
    new FieldReference("invoice_storage", "batch_vouchers", "jsonb", "$.batchedVouchers[*].vendorAddress.city"),
    new FieldReference("invoice_storage", "batch_vouchers", "jsonb", "$.batchedVouchers[*].vendorAddress.stateRegion"),
    new FieldReference("invoice_storage", "batch_vouchers", "jsonb", "$.batchedVouchers[*].vendorAddress.zipCode"),
    new FieldReference("invoice_storage", "batch_vouchers", "jsonb", "$.batchedVouchers[*].vendorAddress.country"),
    new FieldReference("oa", "address", "add_address_line_one"),
    new FieldReference("oa", "address", "add_address_line_two"),
    new FieldReference("oa", "address", "add_city"),
    new FieldReference("oa", "address", "add_region"),
    new FieldReference("oa", "address", "add_postal_code"),
    new FieldReference("oa", "address", "add_country"),
    new FieldReference("organizations_storage", "contacts", "jsonb", "$.addresses[*].addressLine1"),
    new FieldReference("organizations_storage", "contacts", "jsonb", "$.addresses[*].addressLine2"),
    new FieldReference("organizations_storage", "contacts", "jsonb", "$.addresses[*].city"),
    new FieldReference("organizations_storage", "contacts", "jsonb", "$.addresses[*].stateRegion"),
    new FieldReference("organizations_storage", "contacts", "jsonb", "$.addresses[*].zipCode"),
    new FieldReference("organizations_storage", "contacts", "jsonb", "$.addresses[*].country"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.contacts.addresses[*].addressLine1"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.contacts.addresses[*].addressLine2"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.contacts.addresses[*].city"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.contacts.addresses[*].stateRegion"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.contacts.addresses[*].zipCode"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.contacts.addresses[*].country"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.privilegedContacts.addresses[*].addressLine1"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.privilegedContacts.addresses[*].addressLine2"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.privilegedContacts.addresses[*].city"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.privilegedContacts.addresses[*].stateRegion"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.privilegedContacts.addresses[*].zipCode"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.privilegedContacts.addresses[*].country"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.addresses[*].addressLine1"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.addresses[*].addressLine2"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.addresses[*].city"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.addresses[*].stateRegion"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.addresses[*].zipCode"),
    new FieldReference("organizations_storage", "organizations", "jsonb", "$.addresses[*].country"),
    new FieldReference("organizations_storage", "privileged_contacts", "jsonb", "$.addresses[*].addressLine1"),
    new FieldReference("organizations_storage", "privileged_contacts", "jsonb", "$.addresses[*].addressLine2"),
    new FieldReference("organizations_storage", "privileged_contacts", "jsonb", "$.addresses[*].city"),
    new FieldReference("organizations_storage", "privileged_contacts", "jsonb", "$.addresses[*].stateRegion"),
    new FieldReference("organizations_storage", "privileged_contacts", "jsonb", "$.addresses[*].zipCode"),
    new FieldReference("organizations_storage", "privileged_contacts", "jsonb", "$.addresses[*].country"),
    new FieldReference("users", "staging_users", "jsonb", "$.addressInfo.addressLine0"),
    new FieldReference("users", "staging_users", "jsonb", "$.addressInfo.addressLine1"),
    new FieldReference("users", "staging_users", "jsonb", "$.addressInfo.city"),
    new FieldReference("users", "staging_users", "jsonb", "$.addressInfo.province"),
    new FieldReference("users", "staging_users", "jsonb", "$.addressInfo.zip"),
    new FieldReference("users", "staging_users", "jsonb", "$.addressInfo.country"),
    new FieldReference("users", "users", "jsonb", "$.personal.addresses[*].countryId"),
    new FieldReference("users", "users", "jsonb", "$.personal.addresses[*].addressLine1"),
    new FieldReference("users", "users", "jsonb", "$.personal.addresses[*].addressLine2"),
    new FieldReference("users", "users", "jsonb", "$.personal.addresses[*].city"),
    new FieldReference("users", "users", "jsonb", "$.personal.addresses[*].region"),
    new FieldReference("users", "users", "jsonb", "$.personal.addresses[*].postalCode")
  );

  private static final int BATCH_SIZE = 2000;

  @Autowired
  private SharedExecutionContext context;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "Address anonymization",
        "Replaces address elements with randomized realistic-appearing values",
        tenant,
        context,
        JobConfigurationProperty.fromFieldList(ADDRESS_FIELDS, tenant),
        ctx -> {
          List<FieldReference> enabledFields = JobConfigurationProperty.getEnabledFields(ctx.settings()).toList();
          Job job = new Job(ctx, List.of("prepare", "overwrite"));
          job.scheduleParts(
            "prepare",
            enabledFields
              .stream()
              .map(addressField ->
                new BatchGenerationFromTablePart<>(
                  "Prepare batches for address updates in " + addressField.toString(),
                  addressField,
                  BATCH_SIZE,
                  "overwrite",
                  (label, condition, start, end) ->
                    new ReplaceValueFromListPart(
                      "replace %s on %s".formatted(addressField.toString(), label),
                      addressField,
                      condition,
                      replacementValues(addressField, end - start)
                    )
                )
              )
              .toList()
          );
          return job;
        }
      )
    );
  }

  private static List<String> replacementValues(FieldReference fieldReference, int qty) {
    int size = Math.max(1, qty);

    String path = fieldReference.jsonPath();
    if (path != null) {
      if (path.endsWith(".addressLine1") || path.endsWith(".addressLine0")) {
        return RandomValueUtils.streetAddresses(size);
      }
      if (path.endsWith(".addressLine2")) {
        return RandomValueUtils.secondaryAddresses(size);
      }
      if (path.endsWith(".city")) {
        return RandomValueUtils.cities(size);
      }
      if (path.endsWith(".stateRegion") || path.endsWith(".region") || path.endsWith(".province")) {
        return RandomValueUtils.states(size);
      }
      if (path.endsWith(".zipCode") || path.endsWith(".postalCode") || path.endsWith(".zip")) {
        return RandomValueUtils.postalCodes(size);
      }
      if (path.endsWith(".country") || path.endsWith(".countryId")) {
        return RandomValueUtils.countryCodes(size);
      }
      throw new IllegalStateException("No replacement configured for field path: " + path);
    }

    return switch (fieldReference.column()) {
      case "add_address_line_one" -> RandomValueUtils.streetAddresses(size);
      case "add_address_line_two" -> RandomValueUtils.secondaryAddresses(size);
      case "add_city" -> RandomValueUtils.cities(size);
      case "add_region" -> RandomValueUtils.states(size);
      case "add_postal_code" -> RandomValueUtils.postalCodes(size);
      case "add_country" -> RandomValueUtils.countryCodes(size);
      default -> throw new IllegalStateException("No replacement configured for column: " + fieldReference.column());
    };
  }
}
