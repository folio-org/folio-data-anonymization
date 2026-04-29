package org.folio.anonymization.util;

import java.nio.ByteBuffer;
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

  public static List<String> streetAddresses(int qty) {
    return IntStream.range(0, qty).mapToObj(i -> FAKER.address().streetAddress()).toList();
  }

  public static List<String> secondaryAddresses(int qty) {
    return IntStream.range(0, qty).mapToObj(i -> FAKER.address().secondaryAddress()).toList();
  }

  public static List<String> cities(int qty) {
    return IntStream.range(0, qty).mapToObj(i -> FAKER.address().city()).toList();
  }

  public static List<String> states(int qty) {
    return IntStream.range(0, qty).mapToObj(i -> FAKER.address().state()).toList();
  }

  public static List<String> postalCodes(int qty) {
    return IntStream.range(0, qty).mapToObj(i -> FAKER.address().zipCode()).toList();
  }

  public static List<String> countryCodes(int qty) {
    return IntStream.range(0, qty).mapToObj(i -> FAKER.address().countryCode()).toList();
  }

  public static List<String> userAgents(int qty) {
    return IntStream.range(0, qty).mapToObj(i -> FAKER.internet().userAgent()).toList();
  }
}
