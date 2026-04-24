package org.folio.anonymization.controller;

import lombok.extern.log4j.Log4j2;
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

  @EventListener(ApplicationReadyEvent.class)
  public void entryPoint() throws InterruptedException {
    log.info("============================");
    tenantRepository.getModuleTablesWithSizes("fs09000000").forEach(log::info);
    log.info("============================");
  }
}
