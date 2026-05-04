package org.folio.anonymization.view;

import static dev.tamboui.toolkit.Toolkit.panel;
import static dev.tamboui.toolkit.Toolkit.row;
import static dev.tamboui.toolkit.Toolkit.spacer;
import static dev.tamboui.toolkit.Toolkit.waveText;

import dev.tamboui.layout.Flex;
import dev.tamboui.toolkit.element.StyledElement;
import dev.tamboui.toolkit.elements.WaveTextElement;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.controller.TUIState;
import org.folio.anonymization.repository.TenantRepository;
import org.springframework.stereotype.Component;

@Log4j2
@Component
@RequiredArgsConstructor
public class LoadingTenantsView implements TUIView {

  private final TenantRepository tenantRepository;
  private final TUIState state;

  private CompletableFuture<?> tenantFetch = null;

  private final WaveTextElement loadingMessage = waveText("Loading tenant information...");

  @Override
  public StyledElement<?> render() {
    if (tenantFetch == null) {
      log.info("Fetching tenant information...");
      tenantFetch =
        CompletableFuture
          .supplyAsync(() -> tenantRepository.getAllTenants())
          .thenAccept(tenants -> state.completeLoadingTenants(tenants));
    }

    // explode the whole view
    if (tenantFetch.isCompletedExceptionally()) {
      tenantFetch.join();
    }

    return panel("Welcome!", spacer(), row(loadingMessage).flex(Flex.CENTER), spacer()).rounded().fill();
  }
}
