package org.folio.anonymization.view;

import static dev.tamboui.toolkit.Toolkit.list;
import static dev.tamboui.toolkit.Toolkit.panel;
import static dev.tamboui.toolkit.Toolkit.row;
import static dev.tamboui.toolkit.Toolkit.spacer;
import static dev.tamboui.toolkit.Toolkit.text;

import dev.tamboui.layout.Flex;
import dev.tamboui.style.Overflow;
import dev.tamboui.toolkit.element.StyledElement;
import dev.tamboui.toolkit.elements.ListElement;
import dev.tamboui.toolkit.event.EventResult;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.controller.TUIState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class QuitConfirmationView implements TUIView {

  private final TUIState state;

  private final ListElement<?> list;

  @Autowired
  public QuitConfirmationView(TUIState state) {
    this.state = state;
    this.list = list(text("Go back!").centered(), text("Quit").centered());
    this.list.onKeyEvent(e -> {
        if (e.isSelect()) {
          if (list.selected() == 0) {
            this.state.cancelQuitAttempt();
          } else {
            this.state.confirmQuitAttempt();
          }
          return EventResult.HANDLED;
        }
        return EventResult.UNHANDLED;
      });
  }

  @Override
  public StyledElement<?> render() {
    return panel(
      "Confirm",
      spacer(),
      text("Are you sure you want to quit?").bold().centered(),
      spacer(1),
      row(
        spacer(2),
        text(
          "Anonymization has not been completed. Any unfinished jobs may be left in an invalid state and PII may remain in the database."
        )
          .centered()
          .overflow(Overflow.WRAP_WORD),
        spacer(2)
      )
        .flex(Flex.SPACE_AROUND),
      spacer(1),
      row(list).flex(Flex.CENTER),
      spacer()
    )
      .red()
      .rounded()
      .fill();
  }
}
