package org.folio.anonymizer;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import lombok.experimental.UtilityClass;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.PreparedBatch;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.folio.anonymizer.Database.getSchemaName;
import static org.folio.anonymizer.Database.getTableName;


@UtilityClass
public class UserDataOverwriter {

    private static final Logger logger = LogManager.getLogger(UserDataOverwriter.class);
    private static final Faker faker = new Faker();
    private static final int BATCH_SIZE = 100;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public void overwriteUserData() {
        //String selectQuery = String.format("SELECT id, data FROM %s.%s", getSchemaName("mod-users"), TABLE_NAME);
        Jdbi jdbi = Database.getInstance();
//        String uuidValue = "0002cf45-5e88-49e1-90fe-104399884fe9";
//        String selectQuery = String.format("SELECT id, jsonb FROM %s.%s WHERE id = '%s'::uuid LIMIT 1", getSchemaName("mod-users"), getTableName(getSchemaName("mod-users")+"users"), uuidValue);
//        String updateQuery = String.format("UPDATE %s.%s SET jsonb = :newData WHERE id = :id::uuid", getSchemaName("mod-users"), TABLE_NAME);

        String module = "mod-users";
        String tableName = "users";
        String uuidValue = "0fe9";  // Replace with actual UUID value

        String selectQuery = String.format(
                "SELECT id, jsonb FROM %s WHERE id = '%s' LIMIT 1",
                getTableName(module, tableName),
                uuidValue
        );

        System.out.println("the select query is "+selectQuery);
        String updateQuery = String.format(
                "UPDATE %s SET jsonb = to_jsonb(:newData) WHERE id = :id",
                getTableName(module, tableName)
        );

        jdbi.useHandle(handle -> {
            List<Map<String, Object>> users = handle.createQuery(selectQuery)
                    .setFetchSize(BATCH_SIZE)
                    .mapToMap()
                    .list();

            PreparedBatch updateBatch = handle.prepareBatch(updateQuery);
            int count = 0;

            for (Map<String, Object> user : users) {
                UUID id = (UUID) user.get("id"); // Cast to UUID instead of int
                String jsonData = (String) user.get("data");
                String newJsonData = overwriteJsonData(jsonData);

                updateBatch.bind("newData", newJsonData).bind("id", id).add();
                count++;

                if (count % BATCH_SIZE == 0) {
                    updateBatch.execute();
                    updateBatch = handle.prepareBatch(updateQuery);
                }
            }
            if (count % BATCH_SIZE != 0) {
                updateBatch.execute();
            }

            logger.info("Data overwrite completed for table: {}", getTableName(module, tableName));
        });
    }

    private String overwriteJsonData(String jsonData) {
        Map<String, Object> dataMap = new HashMap<>();
        try {
            dataMap = objectMapper.readValue(jsonData, new TypeReference<Map<String, Object>>() {
            });
        } catch (Exception e) {
            logger.error("Failed to parse JSON data: " + jsonData, e);
            return jsonData;
        }
        anonymizeUserData(dataMap);
        try {
            return objectMapper.writeValueAsString(dataMap);
        } catch (Exception e) {
            logger.error("Failed to convert map to JSON: " + dataMap, e);
            return jsonData;
        }
    }

    private void anonymizeUserData(Map<String, Object> dataMap) {
        if (dataMap.containsKey("users")) {
            List<Map<String, Object>> users = (List<Map<String, Object>>) dataMap.get("users");
            for (Map<String, Object> user : users) {
                user.put("username", faker.name().username());
                user.put("personal", anonymizePersonalData((Map<String, Object>) user.get("personal")));
            }
        }
    }

    private Map<String, Object> anonymizePersonalData(Map<String, Object> personalData) {
        if (personalData != null) {
            personalData.put("lastName", faker.name().lastName());
            personalData.put("firstName", faker.name().firstName());
            personalData.put("middleName", faker.name().nameWithMiddle());
            personalData.put("barcode", faker.lorem().characters());
            personalData.put("externalSystemId", faker.idNumber().valid());
            personalData.put("dateOfBirth", faker.date().birthday().toString());
            personalData.put("phone", faker.phoneNumber().phoneNumber());
            personalData.put("mobilePhone", faker.phoneNumber().cellPhone());
            personalData.put("email", faker.internet().emailAddress());

            // Handle 'addresses' array
            List<Map<String, Object>> addresses = (List<Map<String, Object>>) personalData.get("addresses");
            if (addresses != null && !addresses.isEmpty()) {
                for (Map<String, Object> address : addresses) {
                    address.put("city", faker.address().city());
                    address.put("region", faker.address().state());
                    address.put("countryId", faker.address().country());
                    address.put("postalCode", faker.address().zipCode());
                    address.put("addressLine1", faker.address().streetAddress());
                }
            }
        }
        return personalData;
    }
}
