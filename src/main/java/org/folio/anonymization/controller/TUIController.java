package org.folio.anonymization.controller;

import static dev.tamboui.toolkit.Toolkit.column;
import static dev.tamboui.toolkit.Toolkit.flow;
import static dev.tamboui.toolkit.Toolkit.panel;
import static dev.tamboui.toolkit.Toolkit.row;
import static dev.tamboui.toolkit.Toolkit.text;

import dev.tamboui.toolkit.app.ToolkitApp;
import dev.tamboui.toolkit.element.Element;
import dev.tamboui.toolkit.event.EventResult;
import dev.tamboui.tui.TuiConfig;
import java.time.Duration;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.controller.TUIState.State;
import org.folio.anonymization.view.EndView;
import org.folio.anonymization.view.JobConfigurationView;
import org.folio.anonymization.view.JobExecutionView;
import org.folio.anonymization.view.LoadingJobsView;
import org.folio.anonymization.view.LoadingTenantsView;
import org.folio.anonymization.view.QuitConfirmationView;
import org.folio.anonymization.view.ShutdownView;
import org.folio.anonymization.view.StartJobsView;
import org.folio.anonymization.view.TUIView;
import org.folio.anonymization.view.TenantSelectionView;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Controller;

@Log4j2
@Controller
@AllArgsConstructor
public class TUIController extends ToolkitApp {

  private final ApplicationContext ctx;

  private final LoadingTenantsView loadingTenantsView;
  private final TenantSelectionView tenantSelectionView;
  private final LoadingJobsView loadingJobsView;
  private final JobConfigurationView jobConfigurationView;
  private final StartJobsView startJobsView;
  private final JobExecutionView jobExecutionView;
  private final EndView endView;
  private final QuitConfirmationView quitConfirmationView;
  private final ShutdownView shutdownView;

  private final TUIState state;

  @Override
  protected TuiConfig configure() {
    return TuiConfig.builder().tickRate(Duration.ofMillis(40)).mouseCapture(true).build();
  }

  @Override
  protected Element render() {
    return column(this.getView().render(this.runner()), this.keyOptions())
      .onKeyEvent(e -> {
        if (e.isQuit()) {
          if (state.getState() == State.END) {
            this.quit();
          } else {
            state.attemptToQuit();
          }
          return EventResult.HANDLED;
        }
        return EventResult.UNHANDLED;
      });
  }

  protected Element keyOptions() {
    if (state.getState() == State.SHUTTING_DOWN || state.getState() == State.QUIT_CONFIRMATION) {
      return row();
    }

    return panel(
      flow()
        .spacing(2)
        .rowSpacing(0)
        .add(text("[^C or Q] Quit").red().bold())
        .add(this.getView().getHotkeys(this.runner()).stream().filter(Objects::nonNull).toArray(Element[]::new))
    )
      .rounded()
      .focusable(false);
  }

  protected TUIView getView() {
    return switch (state.getState()) {
      case INIT -> loadingTenantsView;
      case TENANT_SELECTION -> tenantSelectionView;
      case JOB_LOADING -> loadingJobsView;
      case JOB_CONFIGURATION -> jobConfigurationView;
      case JOB_KICKOFF -> startJobsView;
      case JOB_EXECUTION -> jobExecutionView;
      case END -> endView;
      case QUIT_CONFIRMATION -> quitConfirmationView;
      case SHUTTING_DOWN -> shutdownView;
      default -> throw new IllegalStateException("Unexpected state: " + state.getState());
    };
  }

  @EventListener(ApplicationReadyEvent.class)
  public void entryPoint() throws Exception {
    System.err.println("You should see an interface shortly...");
    this.run();
  }

  @Override
  protected void onStop() {
    System.err.println("Shutting down... (safe to Ctrl+C if this takes too long)");
    SpringApplication.exit(ctx, () -> 0);
  }
}
