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
import java.util.List;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.controller.TUIState.State;
import org.folio.anonymization.view.LoadingTenantsView;
import org.folio.anonymization.view.QuitConfirmationView;
import org.folio.anonymization.view.ShutdownView;
import org.folio.anonymization.view.TenantSelectionView;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Controller;

@Log4j2
@Controller
@AllArgsConstructor
public class TUIController extends ToolkitApp {

  private final LoadingTenantsView loadingTenantsView;
  private final TenantSelectionView tenantSelectionView;
  private final QuitConfirmationView quitConfirmationView;
  private final ShutdownView shutdownView;

  private final TUIState state;

  @Override
  protected TuiConfig configure() {
    return TuiConfig.builder().tickRate(Duration.ofMillis(40)).mouseCapture(true).build();
  }

  @Override
  protected Element render() {
    return column(
      switch (state.getState()) {
        case INIT -> loadingTenantsView.render();
        case TENANT_SELECTION -> tenantSelectionView.render();
        case QUIT_CONFIRMATION -> quitConfirmationView.render();
        case SHUTTING_DOWN -> shutdownView.render(this::quit);
        default -> panel("tbd").fill();
      },
      this.keyOptions()
    )
      .onKeyEvent(e -> {
        if (e.isQuit()) {
          state.attemptToQuit();
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
        .add(
          (
            switch (state.getState()) {
              case INIT -> List.of();
              case TENANT_SELECTION -> tenantSelectionView.getHotkeys();
              default -> List.of(text("[NEED hotkey impl FOR " + state.getState() + "]").cyan().reversed().bold());
            }
          ).stream()
            .filter(Objects::nonNull)
            .toArray(Element[]::new)
        )
    )
      .rounded()
      .focusable(false);
  }

  @EventListener(ApplicationReadyEvent.class)
  public void entryPoint() throws Exception {
    this.run();
  }
}
