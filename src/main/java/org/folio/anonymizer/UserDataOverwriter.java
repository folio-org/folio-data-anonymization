import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.PreparedBatch;
import org.jdbi.v3.core.statement.StatementContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UserDataOverwriter {

    private static final Logger logger = LogManager.getLogger(UserDataOverwriter.class);
    private static final Faker faker = new Faker();
    private static final int BATCH_SIZE = 100;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public void overwriteUserData() {
        Jdbi jdbi = Database.getInstance();
        String schemaName = Database.getSchemaName("mod_users");
        String tableName = Database.getTableName("mod_users", "users");

        jdbi.useTransaction(handle -> {
            handle.createQuery("SELECT id, data FROM " + tableName)
                    .mapToMap()
                    .forEach(row -> {
                        Integer id = (Integer) row.get("id");
                        String jsonData = (String) row.get("data");
                        String newJsonData = overwriteJsonData(jsonData);

                        handle.createUpdate("UPDATE " + tableName + " SET jsonb = :data WHERE id = :id")
                                .bind("data", newJsonData)
                                .bind("id", id)
                                .execute();
                    });
        });

        logger.info("Data overwrite completed for table: {}", tableName);
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
            personalData.put("middleName", faker.name().nameSuffix());
            personalData.put("dateOfBirth", faker.date().birthday().toString());
            personalData.put("phone", faker.phoneNumber().phoneNumber());
            personalData.put("mobilePhone", faker.phoneNumber().cellPhone());
            personalData.put("email", faker.internet().emailAddress());
        }
        return personalData;
    }
}
