package org.folio.anonymization.view;

import static dev.tamboui.toolkit.Toolkit.panel;
import static dev.tamboui.toolkit.Toolkit.spacer;
import static dev.tamboui.toolkit.Toolkit.waveText;

import dev.tamboui.toolkit.app.ToolkitRunner;
import dev.tamboui.toolkit.element.StyledElement;
import dev.tamboui.toolkit.elements.WaveTextElement;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.controller.TUIState;
import org.springframework.stereotype.Component;

@Log4j2
@Component
@RequiredArgsConstructor
public class ShutdownView implements TUIView {

  private final TUIState state;

  private final WaveTextElement shuttingDownMessage = waveText("Shutting down...");

  @Override
  public StyledElement<?> render(ToolkitRunner runner) {
    // intentionally gets before setting to lag behind by a cycle,
    // ensuring we render the nice goodbye text before we actually quit
    if (state.isReadyToQuit()) {
      runner.quit();
    }
    if (!state.isReadyToQuit()) {
      this.state.setReadyToQuit(true);
    }

    return panel("Goodbye!", spacer(), shuttingDownMessage, spacer()).rounded().fill();
  }
}
