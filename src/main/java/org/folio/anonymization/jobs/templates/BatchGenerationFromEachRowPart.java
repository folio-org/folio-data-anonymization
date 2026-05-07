package org.folio.anonymization.jobs.templates;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.domain.job.JobPart;
import org.jooq.Record;
import org.jooq.Select;

/**
 * Job part to create a series of batch processing children, each acting on a row from a query.
 */
@Log4j2
public class BatchGenerationFromEachRowPart<T extends Record> extends JobPart {

  private final Select<T> query;
  private final BiConsumer<T, Integer> factory;

  public BatchGenerationFromEachRowPart(String label, Select<T> query, BiConsumer<T, Integer> factory) {
    super(label);
    this.query = query;
    this.factory = factory;
  }

  @Override
  protected void execute() {
    var index = new AtomicInteger(0);
    this.create().fetch(query).stream().forEach(record -> this.factory.accept(record, index.getAndIncrement()));
  }
}
