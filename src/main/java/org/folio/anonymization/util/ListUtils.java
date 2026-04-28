package org.folio.anonymization.util;

import java.util.List;
import lombok.experimental.UtilityClass;

@UtilityClass
public class ListUtils {

  public static <T> int addAndGetIndex(List<T> list, T element) {
    list.add(element);
    return list.size() - 1;
  }
}
