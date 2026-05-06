package org.folio.anonymization.view;

import dev.tamboui.toolkit.app.ToolkitRunner;
import dev.tamboui.toolkit.element.Element;
import dev.tamboui.toolkit.element.StyledElement;
import jakarta.annotation.Nonnull;
import java.util.List;

public interface TUIView {
  /**
   * Renders the application UI.
   * Called each frame to get the current state.
   * <p>
   * Add event handlers to elements using {@code onKeyEvent()} and
   * {@code onMouseEvent()} methods.
   *
   * @return the root element to render
   */
  public StyledElement<?> render(ToolkitRunner runner);

  /**
   * Get available hotkeys, excluding system ones (^C/Q for quit). Null values will automatically
   * be filtered out of this list.
   */
  @Nonnull
  public default List<Element> getHotkeys(ToolkitRunner runner) {
    return List.of();
  }
}
