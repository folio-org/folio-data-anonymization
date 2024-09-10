package com.example;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class UserDataOverwriter {

    private static final Logger logger = LogManager.getLogger(UserDataOverwriter.class);
    private static final Faker faker = new Faker();
    private static final int BATCH_SIZE = 100;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String SCHEMA_NAME = "${tenant}_mod_users"; // Hardcoding for POC
    private static final String TABLE_NAME = "users";

    private static Connection getConnection() {
        Properties properties = new Properties();
        try (InputStream input = UserDataOverwriter.class.getClassLoader().getResourceAsStream("db.properties")) {
            if (input == null) {
                logger.error("Unable to find db.properties");
                return null;
            }
            properties.load(input);

            String url = properties.getProperty("db.url");
            String user = properties.getProperty("db.username");
            String password = properties.getProperty("db.password");

            Class.forName("org.postgresql.Driver");
            return DriverManager.getConnection(url, user, password);
        } catch (IOException | ClassNotFoundException | SQLException e) {
            logger.error("Error connecting to the database", e);
            return null;
        }
    }

    public void overwriteUserData() {
        try (Connection connection = getConnection()) {
            if (connection != null) {
                String selectQuery = String.format("SELECT id, data FROM %s.%s", SCHEMA_NAME, TABLE_NAME);
                String updateQuery = String.format("UPDATE %s.%s SET data = ? WHERE id = ?", SCHEMA_NAME, TABLE_NAME);

                try (PreparedStatement selectStatement = connection.prepareStatement(selectQuery);
                     PreparedStatement updateStatement = connection.prepareStatement(updateQuery)) {

                    int count = 0;

                    try (ResultSet resultSet = selectStatement.executeQuery()) {
                        while (resultSet.next()) {
                            int id = resultSet.getInt("id");
                            String jsonData = resultSet.getString("data");
                            String newJsonData = overwriteJsonData(jsonData);

                            updateStatement.setString(1, newJsonData);
                            updateStatement.setInt(2, id);
                            updateStatement.addBatch();

                            count++;

                            if (count % BATCH_SIZE == 0) {
                                updateStatement.executeBatch();
                            }
                        }
                        updateStatement.executeBatch();
                    }

                    logger.info("Data overwrite completed for table: {}", TABLE_NAME);

                } catch (SQLException e) {
                    logger.error("Error overwriting data for table: " + TABLE_NAME, e);
                }
            }
        } catch (SQLException e) {
            logger.error("Error connecting to the database", e);
        }
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
        // Anonymize "users" field if it exists
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
