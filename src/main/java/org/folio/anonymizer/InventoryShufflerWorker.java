package org.folio.anonymizer;

import static org.folio.anonymizer.Utils.pickRandomFromList;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.PreparedBatch;

@Log4j2
@AllArgsConstructor
public class InventoryShufflerWorker implements Runnable {

  private int workerId;

  @Override
  @SneakyThrows(InterruptedException.class)
  public void run() {
    Thread.currentThread().setName("main-" + workerId);
    int currBatch = 0;

    while (true) {
      List<UUID> availableIds = null;
      List<UUID> toMove = null;
      availableIds = InventoryShuffler.availableDestinationsQueue.take();
      InventoryShuffler.numWorking.incrementAndGet();

      try {
        // end condition
        if (availableIds.isEmpty()) {
          log.info("exiting");
          return;
        }

        InventoryShuffler.inNeedOfMovingLock.acquire();
        toMove = pickRandomFromList(InventoryShuffler.inNeedOfMoving.get(), availableIds.size());
        InventoryShuffler.inNeedOfMovingLock.release();

        if (toMove.isEmpty()) {
          InventoryShuffler.numWorking.decrementAndGet();
          InventoryShuffler.availableDestinationsQueue.add(availableIds);
          if (!Thread.currentThread().getName().endsWith("-zzz")) {
            log.info("None are available to move, sleeping");
            Thread.currentThread().setName("main-" + workerId + "-" + currBatch + "-zzz");
          }
          Thread.sleep(1000);
          continue;
        } else {
          Thread.currentThread().setName("main-" + workerId + "-" + currBatch);
        }

        if (toMove.size() > availableIds.size()) {
          InventoryShuffler.inNeedOfMovingLock.acquire();
          InventoryShuffler.inNeedOfMoving.get().addAll(toMove.subList(availableIds.size(), toMove.size()));
          InventoryShuffler.inNeedOfMovingLock.release();
          toMove = toMove.subList(0, availableIds.size());
        } else if (availableIds.size() > toMove.size()) {
          InventoryShuffler.availableDestinationsQueue.add(availableIds.subList(toMove.size(), availableIds.size()));
          availableIds = availableIds.subList(0, toMove.size());
        }

        log.info("Moving a batch of {} {}s", availableIds.size(), InventoryShuffler.currentQueryType.get());
        Instant batchStart = Instant.now();

        runBatch(
          Database.getInstance(),
          InventoryShuffler.QUERIES.get(InventoryShuffler.currentQueryType.get()).getLeft(),
          toMove,
          availableIds
        );

        Duration batchDuration = Duration.between(batchStart, Instant.now());
        // log.info(
        //   "Finished main {} update for batch of {} in {}s",
        //   InventoryShuffler.currentQueryType.get(),
        //   availableIds.size(),
        //   batchDuration.toSeconds()
        // );

        final int currBatchFinal = currBatch;
        final List<UUID> toMoveFinal = toMove;
        final List<UUID> availableIdsFinal = availableIds;

        Instant fkBatchStart = Instant.now();

        InventoryShuffler.QUERIES
          .get(InventoryShuffler.currentQueryType.get())
          .getRight()
          .stream()
          .map(subQuery ->
            InventoryShuffler.fkUpdatePool
              .get(InventoryShuffler.currentQueryType.get() + "-" + subQuery.getLeft())
              .submit(() -> {
                Thread
                  .currentThread()
                  .setName(
                    InventoryShuffler.currentQueryType.get() +
                    "-" +
                    subQuery.getLeft() +
                    "-" +
                    workerId +
                    "-" +
                    currBatchFinal
                  );

                // Instant subBatchStart = Instant.now();
                // log.debug("Starting sub update for batch of {}", availableIdsFinal.size());

                runBatch(Database.getInstance(), subQuery.getRight(), toMoveFinal, availableIdsFinal);

                // Duration subBatchDuration = Duration.between(subBatchStart, Instant.now());
                // log.debug(
                //   "Finished sub update for batch of {} in {}s",
                //   availableIdsFinal.size(),
                //   subBatchDuration.toSeconds()
                // );
                return null;
              })
          )
          // do this to wait to get until all are together; probably a better way but this is fine
          .toList()
          .stream()
          .forEach(Utils::sneakyGet);

        Duration fkBatchDuration = Duration.between(fkBatchStart, Instant.now());
        Duration totalBatchDuration = Duration.between(batchStart, Instant.now());
        log.info(
          "Finished all updates for batch of {} {}s in {}s (main) + {}s (fk) = {}s (total)",
          availableIds.size(),
          InventoryShuffler.currentQueryType.get(),
          batchDuration.toSeconds(),
          fkBatchDuration.toSeconds(),
          totalBatchDuration.plus(fkBatchDuration).toSeconds()
        );

        InventoryShuffler.availableDestinationsQueue.add(toMove);

        currBatch += 1;
      } catch (Exception e) {
        log.error("Error in main worker thread", e);
        if (availableIds != null) {
          InventoryShuffler.availableDestinationsQueue.add(availableIds);
        }
        if (toMove != null) {
          InventoryShuffler.inNeedOfMovingLock.acquire();
          InventoryShuffler.inNeedOfMoving.get().addAll(toMove);
          InventoryShuffler.inNeedOfMovingLock.release();
        }
      }
      InventoryShuffler.numWorking.decrementAndGet();
    }
  }

  private static void runBatch(Jdbi instance, String sql, List<UUID> src, List<UUID> dest) {
    instance.inTransaction((Handle handle) -> {
      // we have to do this per-transaction :(
      handle.execute("SET session_replication_role = 'replica';");
      // handle.execute("SET synchronous_commit = 'off';");

      PreparedBatch batch = handle.prepareBatch(sql);

      for (int i = 0; i < src.size(); i++) {
        batch.bind("srcId", src.get(i)).bind("destId", dest.get(i)).add();
      }

      return batch.execute();
    });
  }
}
