package org.folio.anonymization.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;
import lombok.experimental.UtilityClass;
import net.datafaker.Faker;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.lang3.StringUtils;

@UtilityClass
public class RandomValueUtils {

  // defining static ones instead of allowing Faker to fill this in as our anonymized system may
  // attempt to send emails and we want to ensure we don't accidentally hit a real inbox.
  private static final List<String> EMAIL_DOMAINS = List.of(
    "@example.com",
    "@library.example.com",
    "@institution.example.com",
    "@folio.example.org"
  );

  private static final Faker FAKER = new Faker();
  private static final Base64.Encoder B64 = Base64.getEncoder();
  private static final Base64.Encoder B64_NO_PADDING = B64.withoutPadding();
  private static final Base32 B32 = new Base32();

  public static Function<Integer, List<String>> codeLikeValueGenerator(int startSeed) {
    return qty ->
      IntStream
        .range(startSeed, startSeed + qty)
        .mapToObj(i ->
          // hex digits for entropy + B32 for guaranteed uniqueness (minus padding)
          FAKER.random().hex(6) +
          StringUtils.chop(StringUtils.stripStart(B32.encodeToString(ByteBuffer.allocate(4).putInt(i).array()), "A"))
        )
        .toList();
  }

  public static String randomArrayEntrySql(String... options) {
    return "('{\"%s\"}'::text[])[floor(random() * %d + 1)]".formatted(String.join("\",\"", options), options.length);
  }

  public static String randomArrayEntryToJsonbSql(String... options) {
    return "to_jsonb(%s)".formatted(randomArrayEntrySql(options));
  }

  public static List<String> emails(int qty) {
    return IntStream
      .range(0, qty)
      .mapToObj(i -> FAKER.credentials().username() + EMAIL_DOMAINS.get(FAKER.random().nextInt(EMAIL_DOMAINS.size())))
      .toList();
  }

  public static List<String> loadValuesFromResource(String resourcePath) {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    InputStream stream = classLoader.getResourceAsStream(resourcePath);
    if (stream == null) {
      throw new IllegalStateException("Resource file not found: " + resourcePath);
    }

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
      List<String> values = reader
        .lines()
        .map(String::trim)
        .filter(line -> !line.isEmpty())
        .filter(line -> !line.startsWith("#"))
        .toList();
      if (values.isEmpty()) {
        throw new IllegalStateException("Resource file is empty: " + resourcePath);
      }
      return values;
    } catch (IOException e) {
      throw new IllegalStateException("Failed to read resource file: " + resourcePath, e);
    }
  }

  public static List<String> userAgents(int qty) {
    return IntStream.range(0, qty).mapToObj(i -> FAKER.internet().userAgent()).toList();
  }
}
