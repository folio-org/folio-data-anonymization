package org.folio.anonymization.view;

import static dev.tamboui.toolkit.Toolkit.column;
import static dev.tamboui.toolkit.Toolkit.lineGauge;
import static dev.tamboui.toolkit.Toolkit.panel;
import static dev.tamboui.toolkit.Toolkit.row;
import static dev.tamboui.toolkit.Toolkit.spacer;
import static dev.tamboui.toolkit.Toolkit.text;
import static dev.tamboui.toolkit.Toolkit.waveText;

import dev.tamboui.layout.Constraint;
import dev.tamboui.layout.Flex;
import dev.tamboui.style.Color;
import dev.tamboui.toolkit.element.StyledElement;
import dev.tamboui.toolkit.elements.WaveTextElement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.tuple.Pair;
import org.folio.anonymization.controller.TUIState;
import org.folio.anonymization.domain.folio.Tenant;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.repository.TenantRepository;
import org.springframework.stereotype.Component;

@Log4j2
@Component
@RequiredArgsConstructor
public class LoadingJobsView implements TUIView {

  private final List<JobFactory> jobFactories;
  private final TenantRepository tenantRepository;
  private final TUIState state;

  private CompletableFuture<?> jobFetch = null;
  private ProgressState progress = new ProgressState();

  private final WaveTextElement loadingMessage = waveText("Analyzing tenant databases...");

  @Override
  public StyledElement<?> render() {
    if (jobFetch == null) {
      log.info("Starting job fetching...");
      jobFetch = CompletableFuture.supplyAsync(this::loadJobsForTenants).thenAccept(state::completeLoadingJobs);
    }

    // explode the whole view
    if (jobFetch.isCompletedExceptionally()) {
      jobFetch.join();
    }

    return panel(
      "Analyzing database",
      spacer(),
      row(loadingMessage).flex(Flex.CENTER),
      spacer(1),
      row(
        spacer(),
        column(lineGauge().ratio(progress.ratio()).filledColor(Color.GREEN)).constraint(Constraint.percentage(80)),
        spacer()
      ),
      text(progress.toString()).green().centered(),
      spacer()
    )
      .rounded()
      .fill();
  }

  public List<Pair<Tenant, List<JobBuilder>>> loadJobsForTenants() {
    List<Pair<Tenant, List<JobBuilder>>> results = new ArrayList<>();

    this.progress.setTotalQty(this.state.getSelectedTenants().size());

    for (int i = 0; i < this.state.getSelectedTenants().size(); i++) {
      Tenant tenant = this.state.getSelectedTenants().get(i);
      this.progress.setCurrentIndex(i);
      this.progress.setCurrentTenant(tenant.id());
      this.progress.setOnDbStage(true);

      TenantExecutionContext context = tenantRepository.getTenantExecutionContext(tenant);

      this.progress.setOnDbStage(false);

      results.add(
        Pair.of(
          tenant,
          jobFactories.stream().map(factory -> factory.getBuilders(context)).flatMap(List::stream).sorted().toList()
        )
      );
    }

    return results;
  }

  @Data
  private class ProgressState {

    boolean isOnDbStage = false;
    String currentTenant = "";
    int currentIndex = 0;
    int totalQty = 1;

    public double ratio() {
      if (isOnDbStage) {
        return (currentIndex + 0.25) / totalQty;
      } else {
        return (currentIndex + 0.75) / totalQty;
      }
    }

    @Override
    public String toString() {
      if (isOnDbStage) {
        return "[%s] Analyzing database (%d/%d)".formatted(currentTenant, currentIndex + 1, totalQty);
      } else {
        return "[%s] Building jobs (%d/%d)".formatted(currentTenant, currentIndex + 1, totalQty);
      }
    }
  }
}
