package org.folio.anonymizer;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.experimental.UtilityClass;
import lombok.extern.log4j.Log4j2;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.SqlLogger;
import org.jdbi.v3.core.statement.StatementContext;

@Log4j2
@UtilityClass
public class Database {

  private static final boolean LOG_QUERIES = false;

  private static Jdbi instance = null;

  // adjusts the true entropy of our randomization.
  // batch per query defines the number of subqueries at once, more is faster, but may make postgres mad
  // per batch subquery is the number of rows in each subquery, more is faster, at the expense of good randomness.
  private static final int RANDOM_BATCH_QUERY_QTY = 5000;
  private static final int RANDOM_PER_BATCH_SUBQUERY = 5;

  public static Jdbi getInstance() {
    if (instance != null) {
      return instance;
    }

    // create JDBI from env vars for postgres
    String host = System.getenv("DB_HOST");
    String port = System.getenv("DB_PORT");
    String user = System.getenv("DB_USER");
    String pass = System.getenv("DB_PASS");
    String db = System.getenv("DB_NAME");

    String url = "jdbc:postgresql://" + host + ":" + port + "/" + db;
    log.info("Connecting to DB {}", url);

    instance = Jdbi.create(url, user, pass);
    if (LOG_QUERIES) {
      instance.setSqlLogger(new DBLogger());
    }

    return instance;
  }

  public static String getSchemaName(String module) {
    return System.getenv("TENANT") + "_" + module.replace("-", "_");
  }

  public static String getTableName(String module, String table) {
    return getSchemaName(module) + "." + table;
  }

  public static List<UUID> getRandomIds(String module, String table, int count, String extraCondition) {
    return getInstance()
      .withHandle((Handle handle) -> {
        List<UUID> ids = new ArrayList<>();

        while (ids.size() < count) {
          String query = IntStream
            .range(
              0,
              Math.max(2, Math.min(RANDOM_BATCH_QUERY_QTY, (count - ids.size()) / RANDOM_PER_BATCH_SUBQUERY + 1))
            )
            // using something like ORDER BY random() is slow for large tables, as it does a full scan each time :(
            // and one big ORDER BY random() is bad, as it prevents duplicates
            .mapToObj(i -> UUID.randomUUID())
            .map(id ->
              "SELECT id FROM %s WHERE id >= '%s'::uuid AND (%s) ORDER BY id LIMIT %d".formatted(
                  getTableName(module, table),
                  id,
                  extraCondition != null ? extraCondition : "1=1",
                  RANDOM_PER_BATCH_SUBQUERY
                )
            )
            .collect(Collectors.joining(") UNION ("));

          List<UUID> res = handle
            .createQuery("(%s) LIMIT %d".formatted(query, count - ids.size()))
            .mapTo(UUID.class)
            .list();
          log.info("Got {} ids", res.size());
          ids.addAll(res);
        }

        return ids;
      });
  }

  public static List<UUID> getRandomIds(String module, String table, int count) {
    return getRandomIds(module, table, count, null);
  }

  @Log4j2
  static class DBLogger implements SqlLogger {

    @Override
    public void logAfterExecution(StatementContext context) {
      log.debug(
        "Executed in {} '{}' with parameters '{}'",
        () -> format(Duration.between(context.getExecutionMoment(), context.getCompletionMoment())),
        () -> context.getParsedSql().getSql(),
        () -> context.getBinding()
      );
    }

    @Override
    public void logException(StatementContext context, SQLException ex) {
      log.error(
        "Exception while executing '{}' with parameters '{}'",
        context.getParsedSql().getSql(),
        context.getBinding(),
        ex
      );
    }

    private static String format(Duration duration) {
      final long totalSeconds = duration.getSeconds();
      final long h = totalSeconds / 3600;
      final long m = (totalSeconds % 3600) / 60;
      final long s = totalSeconds % 60;
      final long ms = duration.toMillis() % 1000;
      return String.format("%d:%02d:%02d.%03d", h, m, s, ms);
    }
  }
}
