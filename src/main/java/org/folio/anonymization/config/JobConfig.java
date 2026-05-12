package org.folio.anonymization.config;

import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.domain.job.Job.JobPartRunner;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.jooq.DSLContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Log4j2
@Configuration
public class JobConfig {

  public static final int BATCH_SIZE = 10_000;
  public static final int INSERT_BATCH_SIZE = 5_000;

  private static final Map<String, Integer> STAGE_PRIORITIES = Map.ofEntries(
    // preparation of any kind (gets us a more accurate part count quicker)
    Map.entry("prepare", 0),
    Map.entry("prepare-enumerate-rmb", 0),
    Map.entry("prepare-picklist-values", 0),
    Map.entry("generate-new-values-prep", 0),
    Map.entry("apply-new-values-prep", 0),
    Map.entry("prepare-replace-pictures", 0),
    Map.entry("enumerate-prep", 0),
    // pulling data out to be tabulated
    Map.entry("enumerate-correlated", 1),
    Map.entry("enumerate-independent", 1),
    Map.entry("enumerate", 1),
    // generation
    Map.entry("generate-new-values", 2),
    Map.entry("correlate-new-values", 2),
    // actual modification
    Map.entry("act", 3),
    Map.entry("truncate", 3),
    Map.entry("redact", 3),
    Map.entry("overwrite", 3),
    Map.entry("overwrite-picklist-values", 3),
    Map.entry("apply-new-values", 3),
    Map.entry("update-configuration", 3),
    Map.entry("update-settings", 3),
    Map.entry("replace-pictures", 3),
    // cleanup
    Map.entry("cleanup", 4)
  );

  @Bean
  public ThreadPoolExecutor executor() {
    return new ThreadPoolExecutor(
      100,
      100,
      0L,
      TimeUnit.MILLISECONDS,
      new PriorityBlockingQueue<Runnable>(
        11,
        (a, b) -> {
          if (a instanceof JobPartRunner partA && b instanceof JobPartRunner partB) {
            log.info("Comparing parts '{}' and '{}'", partA.task().getStage(), partB.task().getStage());
            return Integer.compare(
              STAGE_PRIORITIES.getOrDefault(partA.task().getStage(), -1),
              STAGE_PRIORITIES.getOrDefault(partB.task().getStage(), -1)
            );
          } else if (a instanceof JobPartRunner) {
            return 1;
          } else if (b instanceof JobPartRunner) {
            return -1;
          } else {
            return 0;
          }
        }
      )
    );
  }

  @Bean
  public SharedExecutionContext sharedExecutionContext(DSLContext create, ThreadPoolExecutor executor) {
    return new SharedExecutionContext(create, executor);
  }
}
