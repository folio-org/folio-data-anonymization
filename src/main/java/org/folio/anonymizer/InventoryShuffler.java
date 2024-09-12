package org.folio.anonymizer;

import static org.folio.anonymizer.Utils.killExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.tuple.Pair;

@Log4j2
@UtilityClass
public class InventoryShuffler {

  private static final int CONCURRENT_VOLUME_EA = 1500;
  private static final int CONCURRENT_THREADS = 40;
  private static final int CONCURRENT_SUBTHREADS = CONCURRENT_THREADS;

  private static final String SCHEMA = "mod-inventory-storage";
  private static final String INSTANCES_TABLE = Database.getTableName(SCHEMA, "instance");
  private static final String HOLDINGS_TABLE = Database.getTableName(SCHEMA, "holdings_record");
  private static final String ITEMS_TABLE = Database.getTableName(SCHEMA, "item");
  private static final String INSTANCE_SOURCE_MARC_TABLE = Database.getTableName(SCHEMA, "instance_source_marc");
  private static final String PRECEDING_SUCCEEDING_TITLE_TABLE = Database.getTableName(
    SCHEMA,
    "preceding_succeeding_title"
  );
  private static final String INSTANCE_RELATIONSHIP_TABLE = Database.getTableName(SCHEMA, "instance_relationship");
  private static final String BOUND_WITH_PART_TABLE = Database.getTableName(SCHEMA, "bound_with_part");

  private static final String INSTANCE_UPDATE_QUERY =
    "UPDATE %s SET id = :destId, jsonb = jsonb_set(jsonb, '{id}', to_jsonb(:destId)) WHERE id = :srcId".formatted(
        INSTANCES_TABLE
      );
  private static final List<Pair<String, String>> INSTANCE_SUBUPDATE_QUERIES = List.of(
    Pair.of(
      "precedinginstanceid",
      "UPDATE %s SET precedinginstanceid = :destId, jsonb = jsonb_set(jsonb, '{precedingInstanceId}', to_jsonb(:destId)) WHERE precedinginstanceid = :srcId".formatted(
          PRECEDING_SUCCEEDING_TITLE_TABLE
        )
    ),
    Pair.of(
      "succeedinginstanceid",
      "UPDATE %s SET succeedinginstanceid = :destId, jsonb = jsonb_set(jsonb, '{succeedingInstanceId}', to_jsonb(:destId)) WHERE succeedinginstanceid = :srcId".formatted(
          PRECEDING_SUCCEEDING_TITLE_TABLE
        )
    ),
    Pair.of(
      "subinstanceid",
      "UPDATE %s SET subinstanceid = :destId, jsonb = jsonb_set(jsonb, '{subInstanceId}', to_jsonb(:destId)) WHERE subinstanceid = :srcId".formatted(
          INSTANCE_RELATIONSHIP_TABLE
        )
    ),
    Pair.of(
      "superinstanceid",
      "UPDATE %s SET superinstanceid = :destId, jsonb = jsonb_set(jsonb, '{superInstanceId}', to_jsonb(:destId)) WHERE superinstanceid = :srcId".formatted(
          INSTANCE_RELATIONSHIP_TABLE
        )
    ),
    Pair.of(
      "sourcemarcid",
      "UPDATE %s SET id = :destId, jsonb = jsonb_set(jsonb, '{id}', to_jsonb(:destId)) WHERE id = :srcId".formatted(
          INSTANCE_SOURCE_MARC_TABLE
        )
    ),
    Pair.of(
      "holdingsref",
      "UPDATE %s SET instanceid = :destId, jsonb = jsonb_set(jsonb, '{instanceId}', to_jsonb(:destId)) WHERE instanceid = :srcId".formatted(
          HOLDINGS_TABLE
        )
    )
  );

  private static final String HOLDING_UPDATE_QUERY =
    "UPDATE %s SET id = :destId, jsonb = jsonb_set(jsonb, '{id}', to_jsonb(:destId)) WHERE id = :srcId".formatted(
        HOLDINGS_TABLE
      );
  private static final List<Pair<String, String>> HOLDING_SUBUPDATE_QUERIES = List.of(
    Pair.of(
      "itemsref",
      "UPDATE %s SET holdingsrecordid = :destId, jsonb = jsonb_set(jsonb, '{holdingsRecordId}', to_jsonb(:destId)) WHERE holdingsrecordid = :srcId".formatted(
          ITEMS_TABLE
        )
    ),
    Pair.of(
      "holdingboundwith",
      "UPDATE %s SET holdingsrecordid = :destId, jsonb = jsonb_set(jsonb, '{holdingsRecordId}', to_jsonb(:destId)) WHERE holdingsrecordid = :srcId".formatted(
          BOUND_WITH_PART_TABLE
        )
    )
  );

  private static final String ITEM_UPDATE_QUERY =
    "UPDATE %s SET id = :destId, jsonb = jsonb_set(jsonb, '{id}', to_jsonb(:destId)) WHERE id = :srcId".formatted(
        ITEMS_TABLE
      );
  private static final List<Pair<String, String>> ITEM_SUBUPDATE_QUERIES = List.of(
    Pair.of(
      "itemboundwith",
      "UPDATE %s SET itemid = :destId, jsonb = jsonb_set(jsonb, '{itemId}', to_jsonb(:destId)) WHERE itemid = :srcId".formatted(
          BOUND_WITH_PART_TABLE
        )
    )
  );

  public static final Map<String, Pair<String, List<Pair<String, String>>>> QUERIES = Map.of(
    "instance",
    Pair.of(INSTANCE_UPDATE_QUERY, INSTANCE_SUBUPDATE_QUERIES),
    "holding",
    Pair.of(HOLDING_UPDATE_QUERY, HOLDING_SUBUPDATE_QUERIES),
    "item",
    Pair.of(ITEM_UPDATE_QUERY, ITEM_SUBUPDATE_QUERIES)
  );

  // + 1 for waiting to finish thread
  private static final ExecutorService mainUpdatePool = Executors.newFixedThreadPool(CONCURRENT_THREADS + 1);
  public static final ConcurrentMap<String, ExecutorService> fkUpdatePool;

  public static final LinkedBlockingQueue<List<UUID>> availableDestinationsQueue = new LinkedBlockingQueue<>();
  public static final Semaphore inNeedOfMovingLock = new Semaphore(1);
  public static final AtomicInteger numWorking = new AtomicInteger();
  public static final AtomicReference<TreeSet<UUID>> inNeedOfMoving = new AtomicReference<>();
  public static final AtomicReference<String> currentQueryType = new AtomicReference<>("instance");

  static {
    fkUpdatePool = new ConcurrentHashMap<>();
    for (Pair<String, String> pair : INSTANCE_SUBUPDATE_QUERIES) {
      fkUpdatePool.put("instance-" + pair.getLeft(), Executors.newFixedThreadPool(CONCURRENT_SUBTHREADS));
    }
    for (Pair<String, String> pair : HOLDING_SUBUPDATE_QUERIES) {
      fkUpdatePool.put("holding-" + pair.getLeft(), Executors.newFixedThreadPool(CONCURRENT_SUBTHREADS));
    }
    for (Pair<String, String> pair : ITEM_SUBUPDATE_QUERIES) {
      fkUpdatePool.put("item-" + pair.getLeft(), Executors.newFixedThreadPool(CONCURRENT_SUBTHREADS));
    }
  }

  @SneakyThrows
  public static void shuffle() {
    for (int i = 0; i < CONCURRENT_THREADS; i++) {
      mainUpdatePool.submit(new InventoryShufflerWorker(i));
    }

    shuffleInstances();
    shuffleHoldings();
    shuffleItems();

    log.info("Shutting down...");
    for (int i = 0; i < CONCURRENT_THREADS; i++) {
      availableDestinationsQueue.add(List.of());
    }
    killExecutor(mainUpdatePool);
  }

  // counting all 10.9M instances took 5s
  // fetching all 10.9M instance IDs took 59s (warm cache)
  private static void shuffleInstances() {
    currentQueryType.set("instance");

    log.info("We have {} instances", count(INSTANCES_TABLE, null));

    TreeSet<UUID> allIds = getIds(INSTANCES_TABLE, null);
    log.info("Obtained IDs of {} instances", allIds.size());

    List<UUID> temporaryIds = getTemporaryIds(allIds, CONCURRENT_THREADS * CONCURRENT_VOLUME_EA);

    inNeedOfMoving.set(allIds);
    log.info("Obtained IDs of {} instances to shuffle", inNeedOfMoving.get().size());

    for (int i = 0; i < CONCURRENT_THREADS; i++) {
      List<UUID> batch = temporaryIds.subList(i * CONCURRENT_VOLUME_EA, (i + 1) * CONCURRENT_VOLUME_EA);
      availableDestinationsQueue.add(batch);
    }

    waitForDone();

    log.info("Sending temporary IDs to get restored");
    inNeedOfMoving.get().addAll(temporaryIds);
    waitForDone();

    log.info("Shutting down executors for instance FK updates...");
    fkUpdatePool
      .keySet()
      .stream()
      .filter(k -> k.startsWith("inst"))
      .map(fkUpdatePool::get)
      .forEach(Utils::killExecutor);

    availableDestinationsQueue.clear();
    log.info("All done moving instances!");
  }

  // counting all 11.7M took 43s (very cold cache)
  // fetching all 11.7M IDs took 90s (very cold cache)
  private static void shuffleHoldings() {
    currentQueryType.set("holding");

    log.info("We have {} holdings", count(HOLDINGS_TABLE, null));

    TreeSet<UUID> allIds = getIds(HOLDINGS_TABLE, null);
    log.info("Obtained IDs of {} holdings", allIds.size());

    List<UUID> temporaryIds = getTemporaryIds(allIds, CONCURRENT_THREADS * CONCURRENT_VOLUME_EA);

    inNeedOfMoving.set(allIds);
    log.info("Obtained IDs of {} holdings to shuffle", inNeedOfMoving.get().size());

    for (int i = 0; i < CONCURRENT_THREADS; i++) {
      List<UUID> batch = temporaryIds.subList(i * CONCURRENT_VOLUME_EA, (i + 1) * CONCURRENT_VOLUME_EA);
      availableDestinationsQueue.add(batch);
    }

    waitForDone();

    log.info("Sending temporary IDs to get restored");
    inNeedOfMoving.get().addAll(temporaryIds);
    waitForDone();

    log.info("Shutting down executors for holding FK updates...");
    fkUpdatePool
      .keySet()
      .stream()
      .filter(k -> k.startsWith("hold"))
      .map(fkUpdatePool::get)
      .forEach(Utils::killExecutor);

    availableDestinationsQueue.clear();
    log.info("All done moving holdings!");
  }

  // counting all 9.8M took 15s (very cold cache)
  // fetching all 9.8M IDs took 90s (very cold cache)
  private static void shuffleItems() {
    currentQueryType.set("item");

    log.info("We have {} items", count(ITEMS_TABLE, null));

    TreeSet<UUID> allIds = getIds(ITEMS_TABLE, null);
    log.info("Obtained IDs of {} items", allIds.size());

    List<UUID> temporaryIds = getTemporaryIds(allIds, CONCURRENT_THREADS * CONCURRENT_VOLUME_EA);

    inNeedOfMoving.set(allIds);
    log.info("Obtained IDs of {} items to shuffle", inNeedOfMoving.get().size());

    for (int i = 0; i < CONCURRENT_THREADS; i++) {
      List<UUID> batch = temporaryIds.subList(i * CONCURRENT_VOLUME_EA, (i + 1) * CONCURRENT_VOLUME_EA);
      availableDestinationsQueue.add(batch);
    }

    waitForDone();

    log.info("Sending temporary IDs to get restored");
    inNeedOfMoving.get().addAll(temporaryIds);
    waitForDone();

    log.info("Shutting down executors for item FK updates...");
    fkUpdatePool
      .keySet()
      .stream()
      .filter(k -> k.startsWith("item"))
      .map(fkUpdatePool::get)
      .forEach(Utils::killExecutor);

    availableDestinationsQueue.clear();
    log.info("All done moving items!");
  }

  private static void waitForDone() {
    Utils.sneakyGet(
      mainUpdatePool.submit(
        new Runnable() {
          @Override
          @SneakyThrows(InterruptedException.class)
          public void run() {
            Thread.currentThread().setName("status-update");
            int i = 0;
            while (true) {
              inNeedOfMovingLock.acquire();
              boolean isEmpty = inNeedOfMoving.get().isEmpty();
              inNeedOfMovingLock.release();

              if (isEmpty && numWorking.get() == 0) {
                return;
              }

              Thread.sleep(1000);

              if (i % 5 == 0) {
                i = 0;
                log.info(
                  "in need of moving={}, available destination sets={}, working={}",
                  inNeedOfMoving.get().size(),
                  availableDestinationsQueue.size(),
                  numWorking.get()
                );
              }
              i++;
            }
          }
        }
      )
    );
  }

  private static Integer count(String table, String condition) {
    return Database
      .getInstance()
      .withHandle(handle ->
        handle
          .createQuery("SELECT COUNT(*) FROM %s WHERE %s".formatted(table, condition == null ? "1=1" : condition))
          .mapTo(Integer.class)
          .one()
      );
  }

  private static TreeSet<UUID> getIds(String table, String condition) {
    return new TreeSet<>(
      Database
        .getInstance()
        .withHandle(handle ->
          handle
            .createQuery("SELECT id FROM %s WHERE %s".formatted(table, condition == null ? "1=1" : condition))
            .mapTo(UUID.class)
            .set()
        )
    );
  }

  // find IDs not currently being used by any records
  private static List<UUID> getTemporaryIds(TreeSet<UUID> ids, int num) {
    List<UUID> result = new ArrayList<>();
    for (long i = 0; result.size() != num; i++) {
      UUID candidate = new UUID(0, i);
      if (!ids.contains(candidate)) {
        result.add(candidate);
      }
    }
    return result;
  }
}
