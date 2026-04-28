package org.folio.anonymization.controller;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.repository.TenantRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

@Log4j2
@Service
public class EntryPointController {

  @Autowired
  TenantRepository tenantRepository;

  @Autowired
  List<JobFactory> jobFactories;

  @EventListener(ApplicationReadyEvent.class)
  public void entryPoint() throws InterruptedException {
    log.info("============================");
    TenantExecutionContext tenant = tenantRepository.getTenantExecutionContext(
      tenantRepository.getAllTenants().get("fs09000000")
    );
    List<JobBuilder> builders = jobFactories
      .stream()
      .map(factory -> factory.getBuilders(tenant))
      .flatMap(List::stream)
      .toList();
    List<Job> jobs = builders.stream().map(JobBuilder::build).toList();
    log.info("Created {} jobs", jobs.size());

    log.info("Running jobs based on temporary filter...");
    jobs.stream().filter(job -> job.getName().contains("User agent anonymization")).forEach(Job::execute);

    log.info("========== finished launching jobs ==========");

    // spin forever...
    new CompletableFuture<>().join();
  }
}
