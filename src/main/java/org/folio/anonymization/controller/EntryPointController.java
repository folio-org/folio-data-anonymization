package org.folio.anonymization.controller;

import lombok.extern.log4j.Log4j2;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

@Log4j2
@Service
public class EntryPointController {

  @EventListener(ApplicationReadyEvent.class)
  public void entryPoint() throws InterruptedException {
    log.info("Doing something after startup");
    Thread.sleep(10000);
    log.info("Finished doing something after startup");
  }
}
