package org.folio.anonymization.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.OptionalInt;
import lombok.experimental.UtilityClass;

@UtilityClass
public class ProfilePictureSeedCsvLoader {

  public record SeedValue(byte[] profilePictureBlob, byte[] hmac) {}

  public static List<SeedValue> load(String location) {
    if (location == null || location.isBlank()) {
      throw new IllegalArgumentException(
        "Profile picture seed CSV path is required. Set anonymization.profile-picture.seed-csv-path."
      );
    }

    try (BufferedReader reader = open(location)) {
      List<List<String>> rows = parseCsvRows(reader);
      if (rows.isEmpty()) {
        throw new IllegalArgumentException("Profile picture seed CSV is empty: " + location);
      }

      List<String> header = rows.getFirst();
      int blobIndex = findColumn(header, "profile_picture_blob_b64").orElse(-1);
      if (blobIndex < 0) {
        blobIndex = findColumn(header, "profile_picture_blob").orElse(-1);
      }
      if (blobIndex < 0) {
        throw new IllegalArgumentException("Missing profile picture blob column in seed CSV");
      }

      int hmacIndex = findColumn(header, "hmac_b64").orElse(-1);
      if (hmacIndex < 0) {
        hmacIndex = findColumn(header, "hmac").orElse(-1);
      }
      if (hmacIndex < 0) {
        throw new IllegalArgumentException("Missing hmac column in seed CSV");
      }

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
        throw new IllegalArgumentException("No usable seed rows found in profile picture CSV: " + location);
      }
      return result;
    } catch (IOException e) {
      throw new IllegalStateException("Unable to read profile picture seed CSV from: " + location, e);
    }
  }

  private static BufferedReader open(String location) throws IOException {
    if (location.startsWith("classpath:")) {
      String resource = location.substring("classpath:".length()).replaceFirst("^/", "");
      InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
      if (stream == null) {
        throw new IllegalStateException("Profile picture seed CSV not found on classpath: " + location);
      }
      return new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
    }
    return Files.newBufferedReader(Path.of(location), StandardCharsets.UTF_8);
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

  private static OptionalInt findColumn(List<String> header, String columnName) {
    for (int i = 0; i < header.size(); i++) {
      if (columnName.equalsIgnoreCase(header.get(i).trim())) {
        return OptionalInt.of(i);
      }
    }
    return OptionalInt.empty();
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
