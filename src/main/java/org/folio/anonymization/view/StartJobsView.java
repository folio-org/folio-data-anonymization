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
import dev.tamboui.toolkit.app.ToolkitRunner;
import dev.tamboui.toolkit.element.StyledElement;
import dev.tamboui.toolkit.elements.WaveTextElement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.NotImplementedException;
import org.folio.anonymization.controller.TUIState;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.springframework.stereotype.Component;

@Log4j2
@Component
@RequiredArgsConstructor
public class StartJobsView implements TUIView {

  private final TUIState state;

  private CompletableFuture<?> asyncTask = null;
  private ProgressState progress = new ProgressState();

  private final WaveTextElement loadingMessage = waveText("Starting jobs...");

  @Override
  public StyledElement<?> render(ToolkitRunner runner) {
    if (asyncTask == null) {
      log.info("Starting jobs...");
      asyncTask = CompletableFuture.supplyAsync(this::buildAndStartJobs).thenAccept(this.state::completeJobKickoff);
    }

    // explode the whole view
    if (asyncTask.isCompletedExceptionally()) {
      asyncTask.join();
    }

    return panel(
      "Starting jobs...",
      spacer(),
      row(loadingMessage).flex(Flex.CENTER),
      spacer(1),
      row(
        spacer(),
        column(lineGauge().ratio(progress.ratio()).filledColor(Color.GREEN)).constraint(Constraint.percentage(80)),
        spacer()
      ),
      text(progress.toString()).green().centered(),
      spacer(),
      spacer()
    )
      .rounded()
      .fill();
  }

  private List<Job> buildAndStartJobs() {
    List<JobBuilder> jobsToBuild =
      this.state.getAvailableJobs()
        .values()
        .stream()
        .flatMap(List::stream)
        .filter(j -> j.configuration().stream().anyMatch(JobConfigurationProperty::isOn))
        .sorted()
        .toList();

    this.progress.setTotalQty(Math.max(1, jobsToBuild.size()));

    List<Job> jobs = new ArrayList<>(jobsToBuild.size());
    for (int i = 0; i < jobsToBuild.size(); i++) {
      JobBuilder jobBuilder = jobsToBuild.get(i);
      this.progress.setCurrentJob("[%s] %s".formatted(jobBuilder.tenant().tenant().id(), jobBuilder.name()));
      this.progress.setCurrentIndex(i);

      Job job = jobBuilder.build();

      if (job.getDeferralStage() != -1) {
        throw new NotImplementedException(
          "Deferred jobs are not supported in non-interactive mode (job '%s')".formatted(job.getKey())
        );
      } else {
        job.execute();
      }

      jobs.add(job);

      try {
        // be a little gentle to not completely start slamming the DB
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    return jobs;
  }

  @Data
  private class ProgressState {

    String currentJob = "";
    int currentIndex = 0;
    int totalQty = 1;

    public double ratio() {
      return ((double) currentIndex) / totalQty;
    }

    @Override
    public String toString() {
      return "%s (%d/%d)".formatted(currentJob, currentIndex + 1, totalQty);
    }
  }
}
