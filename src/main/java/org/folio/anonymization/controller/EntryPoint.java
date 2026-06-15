package org.folio.anonymization.controller;

import java.util.Arrays;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Controller;

@Log4j2
@Controller
@AllArgsConstructor
public class EntryPoint {

  private final ApplicationArguments args;

  private final NonInteractiveController nonInteractiveController;
  private final TUIController tuiController;

  @EventListener(ApplicationReadyEvent.class)
  public void entryPoint() throws Exception {
    log.info("Found args: {}", Arrays.asList(args.getSourceArgs()));

    String[] arguments = args.getSourceArgs();

    if (arguments.length == 0) {
      log.info("No arguments provided, starting TUI...");

      System.err.println("You should see an interface shortly...");
      this.tuiController.run();
    } else if (arguments.length == 1) {
      log.info("Argument provided, starting in non-interactive mode...");
      this.nonInteractiveController.setConfigurationFromPath(arguments[0]);
      this.nonInteractiveController.run();
    } else {
      throw log.throwing(
        new IllegalArgumentException(
          "Too many CLI arguments provided. Expected zero (interactive mode) or one (non-interactive mode). Found: " +
          arguments.length
        )
      );
    }
  }
}
