package org.folio.anonymization.config;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Log4j2
@Configuration
public class JobConfig {

  public static final int BATCH_SIZE = 10_000;
  public static final int INSERT_BATCH_SIZE = 5_000;

  @Bean
  public ThreadPoolExecutor executor() {
    return new ThreadPoolExecutor(100, 100, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
  }

  @Bean
  public SharedExecutionContext sharedExecutionContext(
    DSLContext create,
    @Qualifier("keycloakDslContext") DSLContext createKeycloak,
    ThreadPoolExecutor executor
  ) {
    return new SharedExecutionContext(create, createKeycloak, executor);
  }
}
