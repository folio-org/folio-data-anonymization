package org.folio.anonymization.jobs;

import static org.jooq.impl.DSL.field;

import java.util.List;
import java.util.stream.IntStream;
import org.folio.anonymization.domain.db.FieldReference;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.ReplaceJSONBValuePart;
import org.folio.anonymization.jobs.templates.ReplaceStringValuePart;
import org.folio.anonymization.util.RandomValueUtils;
import org.jooq.JSONB;
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

  private static final String STREET_JSONB_SQL = randomJsonbValueFromResource("address-values/street.txt");
  private static final String STREET2_JSONB_SQL = randomJsonbValueFromResource("address-values/street2.txt");
  private static final String CITY_JSONB_SQL = randomJsonbValueFromResource("address-values/city.txt");
  private static final String STATE_JSONB_SQL = randomJsonbValueFromResource("address-values/state.txt");
  private static final String ZIPCODE_JSONB_SQL = randomJsonbValueFromResource("address-values/zipcode.txt");
  private static final String COUNTRY_JSONB_SQL = randomJsonbValueFromResource("address-values/country.txt");

  private static final String STREET_TEXT_SQL = randomTextValueFromResource("address-values/street.txt");
  private static final String STREET2_TEXT_SQL = randomTextValueFromResource("address-values/street2.txt");
  private static final String CITY_TEXT_SQL = randomTextValueFromResource("address-values/city.txt");
  private static final String STATE_TEXT_SQL = randomTextValueFromResource("address-values/state.txt");
  private static final String ZIPCODE_TEXT_SQL = randomTextValueFromResource("address-values/zipcode.txt");
  private static final String COUNTRY_TEXT_SQL = randomTextValueFromResource("address-values/country.txt");

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
          List<String> stages = IntStream.range(0, enabledFields.size()).mapToObj(i -> "overwrite-" + i).toList();
          Job job = new Job(ctx, stages.isEmpty() ? List.of("overwrite") : stages);

          job.scheduleParts("prepare",
            enabledFields.stream().map(field -> 
              new BatchGenerationFromTablePart<>(
                "Prepare batches for address updates in " + field.toString(),
                field,
                BATCH_SIZE,
                "prepare",
                (label, condition, start, end) -> {
                  if (field.jsonPath() != null) {
                    return new ReplaceJSONBValuePart(
                      "replace %s on %s".formatted(field.toString(), label),
                      field,
                      i -> field(replacementJsonbSql(addressField), JSONB.class),
                      condition
                    );
                  } else {
                    return new ReplaceValuePart(
                      "replace %s on %s".formatted(field.toString(), label),
                      field,
                      i -> field(replacementTextSql(addressField), String.class),
                      condition
                    );
                  }
                }
              )
            ).toList());
          return job;
        }
      )
    );
  }

  private static String replacementJsonbSql(FieldReference fieldReference) {
    String path = fieldReference.jsonPath();
    if (path == null) {
      throw new IllegalStateException("Expected JSON path for field: " + fieldReference);
    }
    if (path.endsWith(".addressLine1") || path.endsWith(".addressLine0")) {
      return STREET_JSONB_SQL;
    }
    if (path.endsWith(".addressLine2")) {
      return STREET2_JSONB_SQL;
    }
    if (path.endsWith(".city")) {
      return CITY_JSONB_SQL;
    }
    if (path.endsWith(".stateRegion") || path.endsWith(".region") || path.endsWith(".province")) {
      return STATE_JSONB_SQL;
    }
    if (path.endsWith(".zipCode") || path.endsWith(".postalCode") || path.endsWith(".zip")) {
      return ZIPCODE_JSONB_SQL;
    }
    if (path.endsWith(".country") || path.endsWith(".countryId")) {
      return COUNTRY_JSONB_SQL;
    }
    throw new IllegalStateException("No JSONB replacement configured for field path: " + path);
  }

  private static String replacementTextSql(FieldReference fieldReference) {
    return switch (fieldReference.column()) {
      case "add_address_line_one" -> STREET_TEXT_SQL;
      case "add_address_line_two" -> STREET2_TEXT_SQL;
      case "add_city" -> CITY_TEXT_SQL;
      case "add_region" -> STATE_TEXT_SQL;
      case "add_postal_code" -> ZIPCODE_TEXT_SQL;
      case "add_country" -> COUNTRY_TEXT_SQL;
      default -> throw new IllegalStateException("No text replacement configured for column: " + fieldReference.column());
    };
  }

  private static String randomJsonbValueFromResource(String resourcePath) {
    List<String> values = RandomValueUtils.loadValuesFromResource(resourcePath);
    return RandomValueUtils.randomArrayEntryToJsonbSql(values.toArray(new String[0]));
  }

  private static String randomTextValueFromResource(String resourcePath) {
    List<String> values = RandomValueUtils.loadValuesFromResource(resourcePath);
    return RandomValueUtils.randomArrayEntrySql(values.toArray(new String[0]));
  }
}
