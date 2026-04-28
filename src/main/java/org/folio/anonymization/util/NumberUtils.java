package org.folio.anonymization.util;

import lombok.experimental.UtilityClass;

@UtilityClass
public class NumberUtils {

  public static String abbreviate(int number) {
    if (number <= 0) {
      return "few";
    }
    if (number < 1_000) {
      return "~" + Integer.toString(number, 10);
    }
    if (number < 1_000_000) {
      return String.format("~%.1fk", number / 1_000.0);
    }
    return String.format("~%.1fm", number / 1_000_000.0);
  }
}
