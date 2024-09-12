package org.folio.anonymizer;

import static org.folio.anonymizer.Database.getSchemaName;
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
public class UserDataOverwriter {

  private static final Faker faker = new Faker();
  private static final int BATCH_SIZE = 100;
  private static final ObjectMapper objectMapper = new ObjectMapper();

  public void overwriteUserData() {
    Jdbi jdbi = Database.getInstance();
    String module = "mod-users";
    String tableName = "users";

    String selectQuery = String.format("SELECT id, jsonb::text FROM %s", getTableName(module, tableName));

    log.debug("the select query is " + selectQuery);
    String updateQuery = String.format(
      "UPDATE %s SET jsonb = :newData::jsonb WHERE id = :id",
      getTableName(module, tableName)
    );

    log.debug("the updated query is " + updateQuery);

    jdbi.useHandle(handle -> {
      List<Map<String, Object>> users = handle.createQuery(selectQuery).setFetchSize(BATCH_SIZE).mapToMap().list();

      PreparedBatch updateBatch = handle.prepareBatch(updateQuery);
      int count = 0;

      for (Map<String, Object> user : users) {
        UUID id = (UUID) user.get("id");
        String jsonData = (String) user.get("jsonb");
        String newJsonData = overwriteJsonData(jsonData);

        updateBatch.bind("newData", newJsonData).bind("id", id).add();
        count++;

        if (count % BATCH_SIZE == 0) {
          updateBatch.execute();
          log.info("[user-overwrite] anonymized {}%", String.format("%.2f", count / ((double) users.size()) * 100));
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
    anonymizeUserData(dataMap);
    try {
      return objectMapper.writeValueAsString(dataMap);
    } catch (Exception e) {
      log.error("Failed to convert map to JSON: " + dataMap, e);
      return jsonData;
    }
  }

  private void anonymizeUserData(Map<String, Object> dataMap) {
    dataMap.put("barcode", faker.lorem().characters());
    dataMap.put("externalSystemId", faker.idNumber().valid());

    // Anonymize `personal` data if present
    Map<String, Object> personalData = (Map<String, Object>) dataMap.get("personal");
    if (personalData != null) {
      personalData.put("email", faker.internet().emailAddress());
      personalData.put("lastName", faker.name().lastName());
      personalData.put("firstName", faker.name().firstName());
      personalData.put("preferredFirstName", faker.name().firstName());

      // Handle `addresses` array if present
      List<Map<String, Object>> addresses = (List<Map<String, Object>>) personalData.get("addresses");
      if (addresses != null) {
        for (Map<String, Object> address : addresses) {
          address.put("city", faker.address().city());
          address.put("region", faker.address().state());
          address.put("countryId", faker.address().country());
          address.put("postalCode", faker.address().zipCode());
          address.put("addressLine1", faker.address().streetAddress());
        }
      }
    }
  }
}
