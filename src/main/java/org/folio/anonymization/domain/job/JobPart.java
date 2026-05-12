package org.folio.anonymization.domain.job;

import java.sql.SQLTransientConnectionException;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.exception.UncheckedException;
import org.folio.anonymization.domain.folio.Tenant;
import org.jooq.DSLContext;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.core.retry.RetryTemplate;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.dao.QueryTimeoutException;

@Data
@Log4j2
@SuperBuilder
@RequiredArgsConstructor
public abstract class JobPart implements Runnable {

  private static final RetryTemplate RETRY_TEMPLATE = new RetryTemplate(
    // retry for 30 seconds
    RetryPolicy
      .builder()
      .includes(
        PessimisticLockingFailureException.class,
        DataAccessResourceFailureException.class,
        SQLTransientConnectionException.class,
        QueryTimeoutException.class
      )
      .maxRetries(30)
      .build()
  );

  @ToString.Exclude
  @EqualsAndHashCode.Exclude
  protected Job job;

  protected String stage;
  /** label for the individual part; MUST be unique */
  protected final String label;
  private final AtomicBoolean executing = new AtomicBoolean(false);
  private final AtomicBoolean completed = new AtomicBoolean(false);

  @Override
  public final void run() {
    Thread.currentThread().setName("%s-%s-%s".formatted(job.getName(), stage, label));
    log.info("Job {} stage {}: starting job part: {}", job.getName(), stage, label);
    this.executing.set(true);
    try {
      RETRY_TEMPLATE.execute(() -> {
        try {
          this.execute();
          this.completed.set(true);
        } catch (PessimisticLockingFailureException e) {
          log.warn("Job {} stage {} part {}: lock or connection failure. Retrying...", job.getName(), stage, label);
          throw e;
        }
        return null;
      });
    } catch (Exception e) {
      log.error("Error executing job part: {}", label, e);
      throw new UncheckedException(e);
    } finally {
      this.executing.set(false);
      Thread.currentThread().setName("parked");
    }
  }

  protected abstract void execute();

  // utility decorators to preserve sanity
  protected DSLContext create() {
    return this.job.getContext().executionContext().create();
  }

  protected Tenant tenant() {
    return this.job.getContext().tenant().tenant();
  }
}
