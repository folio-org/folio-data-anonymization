package org.folio.anonymization.config;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.jooq.DSLContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Log4j2
@Configuration
public class JobConfig {

  public static final int BATCH_SIZE = 10_000;
  public static final int INSERT_BATCH_SIZE = 5_000;

  @Bean
  public Executor executor() {
    // TODO: make this size configurable, maybe?
    // and/or based on postgres config `max_connections`?
    return Executors.newFixedThreadPool(100);
  }

  @Bean
  public SharedExecutionContext sharedExecutionContext(DSLContext create, Executor executor) {
    return new SharedExecutionContext(create, executor);
  }
}
