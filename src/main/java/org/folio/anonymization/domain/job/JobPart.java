package org.folio.anonymization.domain.job;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.exception.UncheckedException;
import org.folio.anonymization.domain.folio.Tenant;
import org.jooq.DSLContext;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.core.retry.RetryTemplate;
import org.springframework.dao.PessimisticLockingFailureException;

@Data
@Log4j2
public abstract class JobPart implements Supplier<JobPart> {

  private static final RetryTemplate RETRY_TEMPLATE = new RetryTemplate(
    RetryPolicy.builder().includes(PessimisticLockingFailureException.class).build()
  );

  protected Job job;
  protected String stage;
  /** label for the individual part; MUST be unique */
  protected final String label;
  private final AtomicBoolean started = new AtomicBoolean(false);

  @Override
  public final JobPart get() {
    Thread.currentThread().setName("%s-%s-%s".formatted(job.getName(), stage, label));
    log.info("Job {} stage {}: starting job part: {}", job.getName(), stage, label);
    this.started.set(true);
    try {
      RETRY_TEMPLATE.execute(() -> {
        try {
          this.execute();
        } catch (PessimisticLockingFailureException e) {
          log.warn("Job {} stage {} part {}: Pessimistic locking failure. Retrying...", job.getName(), stage, label);
          throw e;
        }
        return null;
      });
    } catch (Exception e) {
      log.error("Error executing job part: {}", label, e);
      throw new UncheckedException(e);
    } finally {
      this.started.set(false);
      Thread.currentThread().setName("parked");
    }
    return this;
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
