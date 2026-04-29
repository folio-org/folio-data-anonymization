package org.folio.anonymization.domain.job;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.domain.folio.Tenant;
import org.jooq.DSLContext;

@Data
@Log4j2
public abstract class JobPart implements Supplier<JobPart> {

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
      this.execute();
    } catch (Exception e) {
      log.error("Error executing job part: {}", label, e);
      throw e;
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
