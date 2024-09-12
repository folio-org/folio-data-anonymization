package org.folio.anonymizer;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import lombok.experimental.UtilityClass;

@UtilityClass
public class Utils {

  public static synchronized List<UUID> pickRandomFromList(NavigableSet<UUID> src, int qty) {
    List<UUID> result = new ArrayList<>();

    for (int i = 0; i < qty && !src.isEmpty(); i++) {
      UUID id = UUID.randomUUID();
      UUID actual = src.ceiling(id);
      if (actual == null) {
        actual = src.first();
      }
      result.add(actual);
      src.remove(actual);
    }

    return result;
  }

  public static void sneakyGet(Future<?> future) {
    try {
      future.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public static void killExecutor(ExecutorService executor) {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(20, java.util.concurrent.TimeUnit.MINUTES)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
