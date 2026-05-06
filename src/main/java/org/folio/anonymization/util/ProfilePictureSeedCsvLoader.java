package org.folio.anonymization.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import lombok.experimental.UtilityClass;
import org.springframework.core.io.Resource;

@UtilityClass
public class ProfilePictureSeedCsvLoader {

  public record SeedValue(byte[] profilePictureBlob, byte[] hmac) {}

  public static List<SeedValue> load(Resource seedCsvResource) {
    if (seedCsvResource == null) {
      throw new IllegalArgumentException("Profile picture seed CSV resource is required.");
    }

    try (
      BufferedReader reader = new BufferedReader(new InputStreamReader(seedCsvResource.getInputStream(), StandardCharsets.UTF_8))
    ) {
      List<List<String>> rows = parseCsvRows(reader);
      if (rows.isEmpty()) {
        throw new IllegalArgumentException("Profile picture seed CSV is empty.");
      }

      List<String> header = rows.getFirst();
      int blobIndex = findRequiredColumn(
        header,
        List.of("profile_picture_blob_b64", "profile_picture_blob"),
        "Missing profile picture blob column in seed CSV"
      );
      int hmacIndex = findRequiredColumn(header, List.of("hmac_b64", "hmac"), "Missing hmac column in seed CSV");

      List<SeedValue> result = new ArrayList<>();
      for (int i = 1; i < rows.size(); i++) {
        List<String> row = rows.get(i);
        if (row.size() <= Math.max(blobIndex, hmacIndex)) {
          continue;
        }

        byte[] blob = decodeBase64(row.get(blobIndex), "profile_picture_blob_b64", i);
        byte[] hmac = decodeBase64(row.get(hmacIndex), "hmac_b64", i);
        result.add(new SeedValue(blob, hmac));
      }

      if (result.isEmpty()) {
        throw new IllegalArgumentException("No usable seed rows found in profile picture CSV.");
      }
      return result;
    } catch (IOException e) {
      throw new IllegalStateException("Unable to read profile picture seed CSV resource.", e);
    }
  }

  private static List<List<String>> parseCsvRows(BufferedReader reader) throws IOException {
    List<List<String>> rows = new ArrayList<>();
    try (PushbackReader in = new PushbackReader(reader, 2)) {
      List<String> row = new ArrayList<>();
      StringBuilder cell = new StringBuilder();
      boolean inQuotes = false;
      int c;
      while ((c = in.read()) != -1) {
        if (c == '"') {
          if (inQuotes) {
            int next = in.read();
            if (next == '"') {
              cell.append('"');
            } else {
              inQuotes = false;
              if (next != -1) {
                in.unread(next);
              }
            }
          } else {
            inQuotes = true;
          }
          continue;
        }

        if (!inQuotes && c == ',') {
          row.add(cell.toString());
          cell.setLength(0);
          continue;
        }

        if (!inQuotes && (c == '\n' || c == '\r')) {
          if (c == '\r') {
            int next = in.read();
            if (next != '\n' && next != -1) {
              in.unread(next);
            }
          }
          row.add(cell.toString());
          cell.setLength(0);
          if (!(row.size() == 1 && row.getFirst().isBlank())) {
            rows.add(row);
          }
          row = new ArrayList<>();
          continue;
        }

        cell.append((char) c);
      }

      if (cell.length() > 0 || !row.isEmpty()) {
        row.add(cell.toString());
        rows.add(row);
      }
    }
    return rows;
  }

  private static int findRequiredColumn(List<String> header, List<String> columnNames, String errorMessage) {
    for (String columnName : columnNames) {
      int index = findColumn(header, columnName);
      if (index >= 0) {
        return index;
      }
    }
    throw new IllegalArgumentException(errorMessage);
  }

  private static int findColumn(List<String> header, String columnName) {
    for (int i = 0; i < header.size(); i++) {
      if (columnName.equalsIgnoreCase(header.get(i).trim())) {
        return i;
      }
    }
    return -1;
  }

  private static byte[] decodeBase64(String raw, String columnName, int rowIndex) {
    String normalized = raw == null ? "" : raw.replaceAll("\\s+", "");
    if (normalized.isEmpty()) {
      throw new IllegalArgumentException("Empty value for " + columnName + " at row " + rowIndex);
    }
    try {
      return Base64.getDecoder().decode(normalized);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid base64 in " + columnName + " at row " + rowIndex, e);
    }
  }
}
