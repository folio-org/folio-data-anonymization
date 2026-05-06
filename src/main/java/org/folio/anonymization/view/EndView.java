package org.folio.anonymization.view;

import static dev.tamboui.toolkit.Toolkit.panel;
import static dev.tamboui.toolkit.Toolkit.row;
import static dev.tamboui.toolkit.Toolkit.spacer;
import static dev.tamboui.toolkit.Toolkit.spinner;
import static dev.tamboui.toolkit.Toolkit.text;

import dev.tamboui.layout.Flex;
import dev.tamboui.style.Color;
import dev.tamboui.toolkit.app.ToolkitRunner;
import dev.tamboui.toolkit.element.Element;
import dev.tamboui.toolkit.element.StyledElement;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.tuple.Pair;
import org.folio.anonymization.controller.TUIState;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobPart;
import org.springframework.stereotype.Component;

@Log4j2
@Component
@RequiredArgsConstructor
public class EndView implements TUIView {

  private final TUIState state;

  private CompletableFuture<?> produceReport = null;

  private Element report = row(text("Generating report").italic(), spinner().frames(".  ", ".. ", "..."))
    .flex(Flex.CENTER);

  @Override
  public StyledElement<?> render(ToolkitRunner runner) {
    if (produceReport == null) {
      log.info("Generating report...");
      produceReport = CompletableFuture.runAsync(this::produceReport);
    }

    // explode the whole view
    if (produceReport.isCompletedExceptionally()) {
      produceReport.join();
    }

    return panel("All done!").add(this.report).bg(Color.GREEN).rounded().fill();
  }

  private void produceReport() {
    List<JobPart> skippedParts = this.state.getSkippedParts();
    List<Pair<JobBuilder, List<JobConfigurationProperty>>> buildersWithSkippedOptions =
      this.state.getAvailableJobs()
        .values()
        .stream()
        .flatMap(List::stream)
        .map(b -> Pair.of(b, b.configuration().stream().filter(p -> !p.getBooleanValue() && !p.isDisabled()).toList()))
        .filter(b -> !b.getRight().isEmpty())
        .toList();

    log.info("Anonymization completed successfully!");
    log.info("======================== Final report: ========================");
    log.info("Skipped parts that failed:");
    skippedParts.forEach(part -> log.info("- {} (job: {})", part.getLabel(), part.getJob().getName()));
    log.info("Executed jobs:");
    this.state.getJobs().forEach(job -> log.info("- {}", job.getName()));
    log.info("Skipped options:");
    buildersWithSkippedOptions.forEach(p -> {
      JobBuilder b = p.getLeft();
      List<JobConfigurationProperty> opts = p.getRight();
      log.info("- [{}] {} ({} skipped options)", b.tenant().tenant().id(), b.name(), opts.size());
      opts.forEach(opt -> log.info("  - {}", opt.getKey()));
    });

    Element[] skippedPartsForDisplay = skippedParts.isEmpty()
      ? new Element[] { spacer(1), text("none").italic().centered() }
      : skippedParts
        .stream()
        .flatMap(part ->
          Stream.of(spacer(1), text(part.getJob().getName()).centered(), text(part.getLabel()).italic().centered())
        )
        .toArray(Element[]::new);

    Element[] completedJobs =
      this.state.getJobs().stream().map(job -> text(job.getName()).centered()).toArray(Element[]::new);

    this.report =
      new Scrollable()
        .scrollUpIndicator(text("[scroll up to see more...]").dim())
        .scrollDownIndicator(text("[scroll down to see more...]").dim())
        .add(spacer(1))
        .add(text("All done!").centered().bold())
        .add(spacer(1))
        .add(text("You may now safely exit the application.").centered().bold())
        .add(spacer(1))
        .add(text("For a summary, check `logs/application.log` or scroll down:").centered())
        .add(spacer(1))
        .add(text("Failed parts that were skipped (" + skippedParts.size() + "):").centered().bold())
        .add(skippedPartsForDisplay)
        .add(spacer(1))
        .add(text("Jobs executed (" + this.state.getJobs().size() + "):").centered().bold())
        .add(spacer(1))
        .add(completedJobs)
        .add(spacer());
  }

  @Override
  public List<Element> getHotkeys(ToolkitRunner runner) {
    return List.of(text("[↑↓ or scroll] Scroll").bold());
  }
}
