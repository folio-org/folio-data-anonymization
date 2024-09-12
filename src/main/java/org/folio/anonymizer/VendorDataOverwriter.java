package org.folio.anonymizer;

import static org.folio.anonymizer.Database.getTableName;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.experimental.UtilityClass;
import lombok.extern.log4j.Log4j2;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.PreparedBatch;

@Log4j2
@UtilityClass
public class VendorDataOverwriter {

  private static final Faker faker = new Faker();
  private static final int BATCH_SIZE = 100;
  private static final ObjectMapper objectMapper = new ObjectMapper();

  public void overwriteVendorData() {
    Jdbi jdbi = Database.getInstance();
    String module = "mod-organizations_storage";
    String tableName = "organizations";
    //  String uuidValue = "xxx-xxx-xxx";  // Replace with actual UUID value

    String selectQuery = String.format(
      "SELECT id, jsonb::text FROM %s WHERE jsonb->>'isVendor' = 'true'",
      getTableName(module, tableName)
    );

    String updateQuery = String.format(
      "UPDATE %s SET jsonb = :newData::jsonb WHERE id = :id",
      getTableName(module, tableName)
    );

    jdbi.useHandle(handle -> {
      List<Map<String, Object>> organizations = handle
        .createQuery(selectQuery)
        .setFetchSize(BATCH_SIZE)
        .mapToMap()
        .list();

      PreparedBatch updateBatch = handle.prepareBatch(updateQuery);
      int count = 0;

      for (Map<String, Object> vendor : organizations) {
        UUID id = (UUID) vendor.get("id");
        String jsonData = (String) vendor.get("jsonb");
        String newJsonData = overwriteJsonData(jsonData);

        updateBatch.bind("newData", newJsonData).bind("id", id).add();
        count++;

        if (count % BATCH_SIZE == 0) {
          updateBatch.execute();
          log.info(
            "[vendor-overwrite] anonymized {}%",
            String.format("%.2f", count / ((double) organizations.size()) * 100)
          );
          updateBatch = handle.prepareBatch(updateQuery);
        }
      }
      if (count % BATCH_SIZE != 0) {
        updateBatch.execute();
      }

      log.info("Data overwrite completed for table: {}", getTableName(module, tableName));
    });
  }

  private String overwriteJsonData(String jsonData) {
    Map<String, Object> dataMap = new HashMap<>();
    try {
      dataMap = objectMapper.readValue(jsonData, new TypeReference<Map<String, Object>>() {});
    } catch (Exception e) {
      log.error("Failed to parse JSON data: " + jsonData, e);
      return jsonData;
    }
    anonymizeVendorData(dataMap);
    try {
      return objectMapper.writeValueAsString(dataMap);
    } catch (Exception e) {
      log.error("Failed to convert map to JSON: " + dataMap, e);
      return jsonData;
    }
  }

  private void anonymizeVendorData(Map<String, Object> dataMap) {
    // Anonymize top-level fields
    dataMap.put("code", faker.lorem().characters(5, 10));
    dataMap.put("name", faker.company().name());
    dataMap.put("description", faker.lorem().sentence(40));
    anonymizingUrls(dataMap);
    anonymizingAliases(dataMap);
    anonymizingAccounts(dataMap);
    anonymizingAddress(dataMap);
    anonymizingPhoneNumbers(dataMap);
    anonymizingEmails(dataMap);
  }

  // Handle `urls` array
  private static void anonymizingUrls(Map<String, Object> dataMap) {
    List<Map<String, Object>> urls = (List<Map<String, Object>>) dataMap.get("urls");
    if (urls != null) {
      for (Map<String, Object> url : urls) {
        url.put("value", faker.internet().url());
      }
    }
  }

  // Handle `aliases` array
  private static void anonymizingAliases(Map<String, Object> dataMap) {
    List<Map<String, Object>> aliases = (List<Map<String, Object>>) dataMap.get("aliases");
    if (aliases != null) {
      for (Map<String, Object> alias : aliases) {
        alias.put("value", faker.lorem().characters(15));
        alias.put("description", faker.lorem().sentence());
      }
    }
  }

  // Handle `accounts` array
  private static void anonymizingAccounts(Map<String, Object> dataMap) {
    List<Map<String, Object>> accounts = (List<Map<String, Object>>) dataMap.get("accounts");
    if (accounts != null) {
      for (Map<String, Object> account : accounts) {
        account.put("name", faker.company().name());
        account.put("accountNo", faker.number().digits(10));
        account.put("libraryCode", faker.lorem().characters());
        account.put("paymentMethod", faker.options().option("EFT", "Credit Card", "Wire Transfer", "DEBIT", "CREDIT"));
        account.put("libraryEdiCode", faker.lorem().characters());
      }
    }
  }

  // Handle `phoneNumbers` array
  private static void anonymizingPhoneNumbers(Map<String, Object> dataMap) {
    List<Map<String, Object>> phoneNumbers = (List<Map<String, Object>>) dataMap.get("phoneNumbers");
    if (phoneNumbers != null) {
      for (Map<String, Object> phoneNumber : phoneNumbers) {
        phoneNumber.put("phoneNumber", faker.phoneNumber().phoneNumber());
        phoneNumber.put("type", faker.options().option("Fax", "Office", "Home", "Mobile"));
      }
    }
  }

  // Handle `addresses` array
  private static void anonymizingAddress(Map<String, Object> dataMap) {
    List<Map<String, Object>> addresses = (List<Map<String, Object>>) dataMap.get("addresses");
    if (addresses != null) {
      for (Map<String, Object> address : addresses) {
        address.put("city", faker.address().city());
        address.put("country", faker.address().country());
        address.put("zipCode", faker.address().zipCode());
        address.put("language", faker.lorem().word());
        address.put("stateRegion", faker.address().state());
        address.put("addressLine1", faker.address().streetAddress());
        address.put("addressLine2", faker.address().secondaryAddress());
      }
    }
  }

  private static void anonymizingEmails(Map<String, Object> dataMap) {
    List<Map<String, Object>> emails = (List<Map<String, Object>>) dataMap.get("emails");
    if (emails != null) {
      for (Map<String, Object> email : emails) {
        email.put("value", faker.internet().emailAddress());
      }
    }
  }
}
