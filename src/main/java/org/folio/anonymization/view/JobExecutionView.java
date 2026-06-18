package org.folio.anonymization.view;

import static dev.tamboui.toolkit.Toolkit.column;
import static dev.tamboui.toolkit.Toolkit.lazy;
import static dev.tamboui.toolkit.Toolkit.lineGauge;
import static dev.tamboui.toolkit.Toolkit.panel;
import static dev.tamboui.toolkit.Toolkit.row;
import static dev.tamboui.toolkit.Toolkit.spacer;
import static dev.tamboui.toolkit.Toolkit.spinner;
import static dev.tamboui.toolkit.Toolkit.text;
import static dev.tamboui.toolkit.Toolkit.tree;
import static dev.tamboui.toolkit.Toolkit.waveText;

import com.zaxxer.hikari.HikariDataSource;
import dev.tamboui.layout.Constraint;
import dev.tamboui.layout.Flex;
import dev.tamboui.style.Color;
import dev.tamboui.style.Style;
import dev.tamboui.toolkit.Toolkit;
import dev.tamboui.toolkit.app.ToolkitRunner;
import dev.tamboui.toolkit.element.Element;
import dev.tamboui.toolkit.element.StyledElement;
import dev.tamboui.toolkit.elements.ListElement;
import dev.tamboui.toolkit.elements.Panel;
import dev.tamboui.toolkit.elements.Row;
import dev.tamboui.toolkit.elements.TextElement;
import dev.tamboui.toolkit.elements.TreeElement;
import dev.tamboui.toolkit.event.EventResult;
import dev.tamboui.widgets.spinner.SpinnerStyle;
import dev.tamboui.widgets.tree.TreeNode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadPoolExecutor;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.tuple.Pair;
import org.folio.anonymization.controller.TUIState;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobPart;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Log4j2
@Component
@RequiredArgsConstructor
public class JobExecutionView implements TUIView {

  private final HikariDataSource folioDataSource;

  @Qualifier("keycloakDataSource")
  private final HikariDataSource keycloakDataSource;

  private final ThreadPoolExecutor executor;
  private final TUIState state;

  private boolean initialized = false;

  private List<Job> jobs;

  private int view = 0;
  private List<Panel> views;
  private boolean showCompletedJobs = true;

  private Panel failure;

  private void initialize(boolean force) {
    if (!this.initialized || force) {
      this.jobs = this.state.getJobs().stream().sorted().toList();

      this.views = new ArrayList<>();
      this.views.add(
          panel(
            "Summary",
            spacer(),
            row(waveText("Executing %s jobs...".formatted(this.jobs.size()))).flex(Flex.CENTER),
            text(" "),
            row(
              spacer(),
              column(
                lazy(() ->
                  lineGauge()
                    .ratio((double) this.jobs.stream().filter(Job::isCompleted).count() / Math.max(1, this.jobs.size()))
                    .filledColor(Color.GREEN)
                    .unfilledColor(Color.YELLOW)
                )
              )
                .constraint(Constraint.percentage(80)),
              spacer()
            ),
            lazy(() ->
              text("%s jobs completed".formatted(this.jobs.stream().filter(Job::isCompleted).count()))
                .green()
                .centered()
            ),
            spacer()
          )
            .rounded()
            .fill()
        );

      TreeElement<TreeNodeJobReference> tree = tree();
      tree
        .id("job-list-tree")
        .scrollbar()
        .fill()
        .nodeRenderer(this::renderNode)
        .highlightStyle(Style.EMPTY.fg(Color.CYAN).bold());

      for (Job job : jobs) {
        if (job.isCompleted() && !this.showCompletedJobs) {
          continue;
        }

        TreeNode<TreeNodeJobReference> jobNode = TreeNode.of(job.getName(), new TreeNodeJobReference(job, null, null));
        job
          .getStages()
          .forEach(stage -> {
            TreeNode<TreeNodeJobReference> stageNode = TreeNode.of(stage, new TreeNodeJobReference(job, stage, null));
            List<JobPart> parts = job
              .getParts()
              .getOrDefault(stage, new ConcurrentLinkedQueue<>())
              .stream()
              .filter(p -> this.showCompletedJobs || p.getCompleted().get() == false)
              .toList();

            if (parts.isEmpty()) {
              stageNode.leaf();
            } else {
              parts.forEach(part -> {
                TreeNode<TreeNodeJobReference> partNode = TreeNode.of(
                  part.getLabel(),
                  new TreeNodeJobReference(job, stage, part)
                );
                stageNode.add(partNode);
              });
            }
            jobNode.add(stageNode);
          });
        tree.add(jobNode);
      }

      this.views.add(
          panel(
            "All jobs",
            tree,
            row(text("Queued").dim(), text("In progress").yellow(), text("Completed").green()).flex(Flex.SPACE_AROUND),
            row(
              text("Remaining parts are estimates and may increase as a job progresses...").italic().centered(),
              spinner(SpinnerStyle.LINE)
            )
              .flex(Flex.SPACE_BETWEEN)
          )
            .rounded()
            .fill()
        );

      this.initialized = true;
    }
  }

  @Override
  public StyledElement<?> render(ToolkitRunner runner) {
    this.initialize(false);

    this.checkFailedPart();
    this.checkAllCompleted();

    if (this.failure != null) {
      return this.failure;
    } else {
      return column(this.getViewList(), this.views.get(this.view))
        .focusable()
        .fill()
        .onKeyEvent(k -> {
          if (k.isCharIgnoreCase('v')) {
            this.initialize(true);
            this.view = (this.view + 1) % this.views.size();
            return EventResult.HANDLED;
          }
          if (k.isCharIgnoreCase('r')) {
            this.initialize(true);
            return EventResult.HANDLED;
          }
          if (k.isCharIgnoreCase('h')) {
            this.showCompletedJobs = !this.showCompletedJobs;
            this.initialize(true);
            return EventResult.HANDLED;
          }
          if (k.isCharIgnoreCase('<')) {
            int newSize = Math.max(1, this.executor.getMaximumPoolSize() - 10);
            this.executor.setCorePoolSize(newSize);
            this.executor.setMaximumPoolSize(newSize);
            return EventResult.HANDLED;
          }
          if (k.isCharIgnoreCase('>')) {
            int newSize = this.executor.getMaximumPoolSize() + 10;
            this.executor.setMaximumPoolSize(newSize);
            this.executor.setCorePoolSize(newSize);
            return EventResult.HANDLED;
          }
          if (k.isCharIgnoreCase('{')) {
            int newSize = Math.max(1, this.folioDataSource.getMaximumPoolSize() - 10);
            this.folioDataSource.setMaximumPoolSize(newSize);
            this.keycloakDataSource.setMaximumPoolSize(newSize);
            return EventResult.HANDLED;
          }
          if (k.isCharIgnoreCase('}')) {
            int newSize = this.folioDataSource.getMaximumPoolSize() + 10;
            this.folioDataSource.setMaximumPoolSize(newSize);
            this.keycloakDataSource.setMaximumPoolSize(newSize);
            return EventResult.HANDLED;
          }
          return EventResult.UNHANDLED;
        });
    }
  }

  private void checkAllCompleted() {
    synchronized (state) {
      if (!state.isExecutingDeferredJobs() && this.jobs.stream().allMatch(j -> j.isCompleted() || j.isDeferred())) {
        log.info("Starting deferred jobs!");
        this.jobs.stream().filter(j -> j.isDeferred()).forEach(Job::execute);
      } else if (state.isExecutingDeferredJobs() && this.jobs.stream().allMatch(Job::isCompleted)) {
        this.state.finish();
      }
    }
  }

  private Panel getViewList() {
    TextElement[] children = new TextElement[] { text("[Summary]"), text("[All jobs]") };

    children[this.view].cyan().bold();

    Panel panel = panel("Views", row(children).flex(Flex.SPACE_AROUND)).rounded();

    return panel;
  }

  private void checkFailedPart() {
    if (this.failure != null) {
      // already one problem awaiting response, don't recreate view or change to another
      return;
    }

    Optional<Pair<JobPart, Throwable>> failure =
      this.jobs.stream().map(Job::getFailedPart).filter(Optional::isPresent).map(Optional::get).findFirst();
    failure.ifPresent(f -> {
      JobPart part = f.getLeft();
      Throwable error = f.getRight();

      ListElement<?> list = Toolkit.list(text("Restart part").centered(), text("Skip part").centered());
      list.onKeyEvent(e -> {
        if (e.isSelect()) {
          if (list.selected() == 0) {
            part.getJob().executePart(part, true);
          } else {
            part.getJob().skipPart(part);
            this.state.reportSkippedPart(part);
          }
          this.failure = null;
          return EventResult.HANDLED;
        }
        return EventResult.UNHANDLED;
      });

      List<String> exceptionMessages = new ArrayList<>();
      Throwable current = error;
      while (current != null && exceptionMessages.size() < 5) {
        exceptionMessages.add(current.getClass().getName() + ": " + current.getMessage());
        current = current.getCause();
      }

      this.failure =
        panel(
          "Part failed",
          spacer(),
          text("The following part has failed:").centered(),
          text(part.getLabel()).centered().bold(),
          row(
            row(
              text("in stage '"),
              text(part.getStage()).bold(),
              text("' of '"),
              text(part.getJob().getName()).bold(),
              text("'")
            )
          )
            .flex(Flex.SPACE_AROUND),
          spacer(1),
          column(
            exceptionMessages
              .stream()
              .flatMap(s -> {
                s = s.replaceAll("\n", " ");
                List<String> chopped = new ArrayList<>();
                while (s.length() > 80) {
                  int chopAt = s.indexOf(' ', 80);
                  if (chopAt == -1) {
                    chopAt = 80;
                  }
                  chopped.add(s.substring(0, chopAt));
                  s = s.substring(chopAt).trim();
                }
                chopped.add(s);
                return chopped.stream();
              })
              .map(s -> text(s).centered())
              .toArray(Element[]::new)
          ),
          spacer(1),
          list,
          spacer()
        )
          .bg(Color.RED)
          .rounded()
          .fill();
    });
  }

  private StyledElement<?> renderNode(TreeNode<TreeNodeJobReference> node) {
    TreeNodeJobReference ref = node.data();

    if (ref.part() != null) {
      if (this.state.getSkippedParts().contains(ref.part())) {
        return text(ref.part().getLabel()).red();
      } else if (ref.part().getCompleted().get()) {
        return text(ref.part().getLabel()).green();
      } else if (ref.part().getExecuting().get()) {
        return text(ref.part().getLabel()).yellow();
      } else {
        return text(ref.part().getLabel()).dim();
      }
    }
    if (ref.stage() != null) {
      if (ref.job().getCurrentStageIndex() < ref.job().getStages().indexOf(ref.stage())) {
        return text(ref.stage()).dim();
      } else if (ref.job().getCurrentStageIndex() == ref.job().getStages().indexOf(ref.stage())) {
        int totalParts = ref.job().getParts().getOrDefault(ref.stage(), new ConcurrentLinkedQueue<>()).size();
        int inProgressParts = (int) ref
          .job()
          .getParts()
          .getOrDefault(ref.stage(), new ConcurrentLinkedQueue<>())
          .stream()
          .filter(p -> p.getExecuting().get())
          .count();
        int completedParts = (int) ref
          .job()
          .getParts()
          .getOrDefault(ref.stage(), new ConcurrentLinkedQueue<>())
          .stream()
          .filter(p -> p.getCompleted().get())
          .count();

        return row(
          text(ref.stage()).yellow(),
          text(" "),
          getGauge(inProgressParts, completedParts, totalParts),
          text("  ")
        );
      } else {
        return text(ref.stage()).green();
      }
    }
    if (ref.job().isCompleted()) {
      return text(ref.job().getName()).green();
    } else {
      String currentStage = ref
        .job()
        .getStages()
        .get(Math.min(ref.job().getCurrentStageIndex(), ref.job().getStages().size() - 1));
      int totalParts = ref.job().getParts().values().stream().mapToInt(Collection::size).sum();
      int inProgressParts = ref
        .job()
        .getParts()
        .values()
        .stream()
        .mapToInt(parts -> (int) parts.stream().filter(p -> p.getExecuting().get()).count())
        .sum();
      int completedParts = ref
        .job()
        .getParts()
        .values()
        .stream()
        .mapToInt(parts -> (int) parts.stream().filter(p -> p.getCompleted().get()).count())
        .sum();
      return row(
        text(ref.job().getName()).yellow(),
        text(" (" + currentStage + ") ").yellow(),
        getGauge(inProgressParts, completedParts, totalParts),
        text(" ")
      );
    }
  }

  private Element getGauge(int inProgressParts, int completedParts, int totalParts) {
    totalParts = Math.max(totalParts, 1);

    Row gaugeRow = row();
    if (completedParts != 0) {
      gaugeRow.add(
        lineGauge()
          .percent(100)
          .filledColor(Color.GREEN)
          .constraint(
            Constraint.percentage((int) Math.floor(((double) completedParts / Math.max(1, totalParts)) * 100))
          )
      );
    }
    gaugeRow.add(
      lineGauge()
        .ratio((double) inProgressParts / Math.max(1, totalParts - completedParts))
        .filledColor(Color.YELLOW)
        .constraint(
          Constraint.percentage(
            (int) Math.floor(((double) (totalParts - completedParts) / Math.max(1, totalParts)) * 100)
          )
        )
    );

    Row result = row();
    result.add(gaugeRow.fill(), text(" "), text("("));
    if (completedParts > 0 && inProgressParts > 0) {
      result.add(text(completedParts).green(), text("+"), text(inProgressParts).yellow());
    } else if (completedParts > 0) {
      result.add(text(completedParts).green());
    } else if (inProgressParts > 0) {
      result.add(text(inProgressParts).yellow());
    } else {
      result.add(text("0"));
    }
    result.add(text("/"), text(totalParts), text(")"), text(" "));

    return result.fill();
  }

  @Override
  public List<Element> getHotkeys(ToolkitRunner runner) {
    List<Element> hotkeys = new ArrayList<>();

    hotkeys.add(text("[v] Change view").bold());

    if (this.view == 1) {
      hotkeys.add(text("[R] Refresh parts").bold());
      if (this.showCompletedJobs) {
        hotkeys.add(text("[H] Hide completed").bold());
      } else {
        hotkeys.add(text("[H] Show completed").bold());
      }

      hotkeys.add(text("[↑↓] Navigate").bold());
      hotkeys.add(text("[→←] Expand/collapse").bold());
      hotkeys.add(
        row(
          text("[<>] Thread pool size ("),
          text(String.valueOf(this.executor.getMaximumPoolSize())).magenta(),
          text(")")
        )
          .bold()
      );
      hotkeys.add(
        row(
          text("[{}] DB pool size ("),
          text(String.valueOf(this.folioDataSource.getMaximumPoolSize())).magenta(),
          text(")")
        )
          .bold()
      );
    }

    return hotkeys;
  }

  private record TreeNodeJobReference(Job job, String stage, JobPart part) {}
}
