package org.folio.anonymizer;

import java.util.List;
import java.util.UUID;
import lombok.experimental.UtilityClass;
import lombok.extern.log4j.Log4j2;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.statement.PreparedBatch;

@Log4j2
@UtilityClass
public class LoanShuffler {

  private static final int BATCH_SIZE = 1000;
  private static final int SHUFFLE_PERCENTAGE = 20;

  private static final String TABLE_NAME = Database.getTableName("mod-circulation-storage", "loan");

  public static void shuffleLoans() {
    int totalLoans = Database
      .getInstance()
      .withHandle(handle ->
        handle
          .createQuery("SELECT COUNT(*) FROM %s where jsonb->>'userId' is not null".formatted(TABLE_NAME))
          .mapTo(Integer.class)
          .one()
      );

    int numLoansToShuffle = totalLoans * SHUFFLE_PERCENTAGE / 100;

    log.info(
      "We have {} loans with user information; shuffling {}% ({})",
      totalLoans,
      SHUFFLE_PERCENTAGE,
      numLoansToShuffle
    );

    List<UUID> loansToShuffle = Database.getRandomIds(
      "mod-circulation-storage",
      "loan",
      numLoansToShuffle,
      "jsonb->>'userId' is not null"
    );
    List<UUID> usersToAssign = Database.getRandomIds("mod-users", "users", numLoansToShuffle);

    log.info("Gathered IDs, persisting...");

    for (int i = 0; i < numLoansToShuffle; i += BATCH_SIZE) {
      int end = Math.min(i + BATCH_SIZE, numLoansToShuffle);

      List<UUID> loansInBatch = loansToShuffle.subList(i, end);
      List<UUID> usersInBatch = usersToAssign.subList(i, end);

      Database
        .getInstance()
        .withHandle((Handle handle) -> {
          PreparedBatch batch = handle.prepareBatch(
            "UPDATE %s SET jsonb = jsonb_set(jsonb, '{userId}', to_jsonb(:userId)) WHERE id = :loanId".formatted(
                TABLE_NAME
              )
          );

          for (int j = 0; j < loansInBatch.size(); j++) {
            batch.bind("loanId", loansInBatch.get(j)).bind("userId", usersInBatch.get(j)).add();
          }

          batch.execute();

          return null;
        });

      log.info(
        "{}% done, persisted batch {}-{}",
        String.format("%.2f", ((double) i) / numLoansToShuffle * 100),
        i,
        end
      );
    }

    log.info("Done!");
  }
}
