package org.folio.anonymization.config;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobNotifier;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.jooq.DSLContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Log4j2
@Configuration
public class JobConfig {

  @Bean
  public JobNotifier notifier() {
    // TODO: make this fancier, integrate with interface, etc
    return new JobNotifier() {
      @Override
      public void onStatusUpdate(Job job) {
        log.info("Job status update ping received for {}. Job done?={}", job.getName(), job.isDone());
      }
    };
  }

  @Bean
  public Executor executor() {
    // TODO: make this size configurable, maybe?
    // and/or based on postgres config `max_connections`?
    return Executors.newFixedThreadPool(100);
  }

  @Bean
  public SharedExecutionContext sharedExecutionContext(DSLContext create, JobNotifier notifier, Executor executor) {
    return new SharedExecutionContext(create, notifier, executor);
  }
}
