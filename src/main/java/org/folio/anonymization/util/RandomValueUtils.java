package org.folio.anonymization.util;

import com.github.javafaker.Faker;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;
import lombok.experimental.UtilityClass;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.lang3.StringUtils;

@UtilityClass
public class RandomValueUtils {

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
}
