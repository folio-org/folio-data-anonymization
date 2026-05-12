package org.folio.anonymization.domain.job;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.jooq.DSLContext;

public record SharedExecutionContext(DSLContext create, ThreadPoolExecutor executor) {
  public static SharedExecutionContext forTests() {
    return new SharedExecutionContext(
      null,
      new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>())
    );
  }
}
