package org.folio.anonymization.controller;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.repository.TenantRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Log4j2
@Service
public class EntryPointController {

  @Autowired
  TUIController tuiController;

  @Autowired
  TenantRepository tenantRepository;

  @Autowired
  List<JobFactory> jobFactories;

  public void entryPoint() throws Exception {
    tuiController.run();
    log.info("============================");

    TenantExecutionContext tenant = tenantRepository.getTenantExecutionContext(
      tenantRepository.getAllTenants().get("fs09000000")
    );
    List<JobBuilder> builders = jobFactories
      .stream()
      .map(factory -> factory.getBuilders(tenant))
      .flatMap(List::stream)
      .toList();
    log.info("Found {} available jobs", builders.size());

    JobBuilder toTest = builders.stream().filter(b -> b.name().contains("Account number")).findFirst().orElseThrow();
    toTest.configuration().stream().filter(c -> c.getKey().equals("drop-table")).forEach(s -> s.setBooleanValue(false));
    log.info("Running job...");
    toTest.build().execute();

    log.info("========== finished launching jobs ==========");

    // spin forever...
    new CompletableFuture<>().join();
  }
}
