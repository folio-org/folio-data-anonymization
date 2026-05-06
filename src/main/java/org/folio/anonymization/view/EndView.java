package org.folio.anonymization.view;

import static dev.tamboui.toolkit.Toolkit.column;
import static dev.tamboui.toolkit.Toolkit.panel;
import static dev.tamboui.toolkit.Toolkit.row;
import static dev.tamboui.toolkit.Toolkit.spacer;
import static dev.tamboui.toolkit.Toolkit.text;
import static dev.tamboui.toolkit.Toolkit.waveText;

import dev.tamboui.layout.Flex;
import dev.tamboui.style.Color;
import dev.tamboui.toolkit.app.ToolkitRunner;
import dev.tamboui.toolkit.element.Element;
import dev.tamboui.toolkit.element.StyledElement;
import dev.tamboui.toolkit.elements.Column;
import dev.tamboui.toolkit.elements.WaveTextElement;
import java.util.List;
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

  private boolean didDumpLog = false;

  private final WaveTextElement completionMessage = waveText("All done!");

  @Override
  public StyledElement<?> render(ToolkitRunner runner) {
    List<JobPart> skippedParts = this.state.getSkippedParts();
    Column skippedPartsForDisplay = skippedParts.isEmpty()
      ? column(spacer(1), text("none").italic().centered())
      : column(
        skippedParts
          .stream()
          .map(part ->
            column(text(spacer(1), part.getJob().getName()).centered(), text("↳ " + part.getLabel()).centered())
          )
          .toArray(Element[]::new)
      );

    Column completedJobs = column(
      this.state.getJobs().stream().map(job -> text(job.getName()).centered()).toArray(Element[]::new)
    );

    List<Pair<JobBuilder, List<JobConfigurationProperty>>> buildersWithSkippedOptions =
      this.state.getAvailableJobs()
        .values()
        .stream()
        .flatMap(List::stream)
        .map(b -> Pair.of(b, b.configuration().stream().filter(p -> !p.getBooleanValue() && !p.isDisabled()).toList()))
        .filter(b -> !b.getRight().isEmpty())
        .toList();

    Column skippedOptions = buildersWithSkippedOptions.isEmpty()
      ? column(spacer(1), text("none").italic().centered())
      : column(
        buildersWithSkippedOptions
          .stream()
          .map(p ->
            column(text(spacer(1), p.getLeft().name()).centered())
              .add(p.getRight().stream().map(opt -> row(text("↳ "), opt.getLabel())).toArray(Element[]::new))
          )
          .toArray(Element[]::new)
      );

    if (!this.didDumpLog) {
      this.didDumpLog = true;

      log.info("Anonymization completed successfully!");
      log.info("Final report:");
      log.info("Skipped parts that failed:");
      skippedParts.forEach(part -> log.info("- {} (job: {})", part.getLabel(), part.getJob().getName()));
      log.info("Executed jobs:");
      this.state.getJobs().forEach(job -> log.info("- {}", job.getName()));
      log.info("Skipped options:");
      buildersWithSkippedOptions.forEach(p -> {
        JobBuilder b = p.getLeft();
        List<JobConfigurationProperty> opts = p.getRight();
        log.info("- {} ({} skipped options)", b.name(), opts.size());
        opts.forEach(opt -> log.info("  - {}", opt.getLabel()));
      });
    }

    return panel(
      "All done!",
      spacer(),
      row(completionMessage).flex(Flex.CENTER),
      spacer(1),
      text("You may now safely exit the application.").centered().bold(),
      spacer(1),
      text("For a summary, check `logs/application.log` or scroll down:").centered(),
      spacer(1),
      text("Failed parts that were skipped (" + skippedParts.size() + "):").centered().bold(),
      skippedPartsForDisplay,
      spacer(1),
      text("Jobs executed (" + this.state.getJobs().size() + "):").centered().bold(),
      spacer(1),
      completedJobs,
      spacer(1),
      text("Skipped options/jobs (" + buildersWithSkippedOptions.size() + "):").centered().bold(),
      skippedOptions,
      spacer()
    )
      .bg(Color.GREEN)
      .rounded()
      .fill();
  }

  @Override
  public List<Element> getHotkeys(ToolkitRunner runner) {
    return List.of(text("[↑↓ or scroll] Scroll").bold());
  }
}
