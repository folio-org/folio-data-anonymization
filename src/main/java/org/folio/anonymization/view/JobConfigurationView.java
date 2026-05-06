package org.folio.anonymization.view;

import static dev.tamboui.toolkit.Toolkit.column;
import static dev.tamboui.toolkit.Toolkit.list;
import static dev.tamboui.toolkit.Toolkit.panel;
import static dev.tamboui.toolkit.Toolkit.row;
import static dev.tamboui.toolkit.Toolkit.spacer;
import static dev.tamboui.toolkit.Toolkit.text;
import static dev.tamboui.toolkit.Toolkit.tree;

import dev.tamboui.style.Color;
import dev.tamboui.style.Style;
import dev.tamboui.toolkit.app.ToolkitRunner;
import dev.tamboui.toolkit.element.Element;
import dev.tamboui.toolkit.element.StyledElement;
import dev.tamboui.toolkit.elements.ListElement;
import dev.tamboui.toolkit.elements.Row;
import dev.tamboui.toolkit.elements.TreeElement;
import dev.tamboui.toolkit.event.EventResult;
import dev.tamboui.widgets.tree.TreeNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.controller.TUIState;
import org.folio.anonymization.domain.folio.Tenant;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.springframework.stereotype.Component;

@Log4j2
@Component
@RequiredArgsConstructor
public class JobConfigurationView implements TUIView {

  private final TUIState state;

  private boolean initialized = false;

  private List<Tenant> tenants;
  private Map<Tenant, List<JobBuilder>> jobs;
  private List<TreeElement<TreeNodeJobReference>> trees;

  private ListElement<?> tenantList;

  private boolean showConfirmDialog = false;
  private ListElement<?> confirmDialogList;

  private void initialize() {
    if (!this.initialized) {
      this.tenants = this.state.getSelectedTenants();
      this.jobs = this.state.getAvailableJobs();

      this.tenantList = list().id("job-configuration-tenant-list").autoScroll();
      this.trees = new ArrayList<>();
      for (Tenant t : this.tenants) {
        tenantList.add(text(t.id() + "  "));

        TreeElement<TreeNodeJobReference> tree = tree();
        tree
          .id("job-list-tree-" + trees.size())
          .scrollbar()
          .fill()
          .onKeyEvent(k -> {
            if (k.isSelect()) {
              int idx = Math.min(tree.selected(), tree.lastFlatEntries().size() - 1);
              TreeNode<TreeNodeJobReference> node = tree.lastFlatEntries().get(idx).node();
              node.data().toggle();
              return EventResult.HANDLED;
            } else if (k.isChar('*')) {
              tree
                .lastFlatEntries()
                .stream()
                .filter(e -> e.depth() == 0)
                .map(e -> e.node().data())
                .forEach(TreeNodeJobReference::toggle);
              return EventResult.HANDLED;
            }
            return EventResult.UNHANDLED;
          });

        for (JobBuilder job : jobs.get(t)) {
          TreeNode<TreeNodeJobReference> node = TreeNode.of(job.name(), new TreeNodeJobReference(job, null));
          job
            .configuration()
            .forEach(prop -> {
              node.add(TreeNode.of(prop.getKey().toString(), new TreeNodeJobReference(job, prop)).leaf());
            });
          tree.add(node);
        }

        this.trees.add(tree);

        this.confirmDialogList = list(text("Go back").centered(), text("Yes, start").centered());
        this.confirmDialogList.onKeyEvent(e -> {
            if (e.isSelect()) {
              if (this.confirmDialogList.selected() == 0) {
                this.showConfirmDialog = false;
              } else {
                this.state.completeJobConfiguration();
              }
              return EventResult.HANDLED;
            }
            return EventResult.UNHANDLED;
          });
      }

      this.initialized = true;
    }
  }

  @Override
  public StyledElement<?> render(ToolkitRunner runner) {
    this.initialize();

    if (this.showConfirmDialog) {
      return panel(
        "Start",
        spacer(),
        text("Ready to start anonymization?").centered(),
        spacer(1),
        this.confirmDialogList,
        spacer()
      )
        .fg(Color.GREEN)
        .rounded()
        .margin(2)
        .fill();
    } else {
      return panel(
        "Job configuration",
        panel(
          "Tenants",
          tenantList.highlightStyle(
            isTenantListFocused(runner) ? Style.EMPTY.fg(Color.CYAN).reversed() : Style.EMPTY.reversed()
          )
        )
          .id("job-configuration-tenants-panel")
          .focusable()
          .focusedBorderColor(Color.CYAN)
          .rounded(),
        panel(
          "Jobs",
          trees
            .get(tenantList.selected())
            .nodeRenderer(JobConfigurationView::renderNode)
            .highlightStyle(isTenantListFocused(runner) ? Style.EMPTY.bold() : Style.EMPTY.fg(Color.CYAN).bold())
        )
          .id("job-configuration-jobs-panel")
          .focusable()
          .focusedBorderColor(Color.CYAN)
          .rounded()
          .fill()
      )
        .horizontal()
        .rounded()
        .fill()
        .onKeyEvent(k -> {
          if (k.isCharIgnoreCase('g')) {
            this.showConfirmDialog = true;
            return EventResult.HANDLED;
          }
          return EventResult.UNHANDLED;
        });
    }
  }

  @Override
  public List<Element> getHotkeys(ToolkitRunner runner) {
    List<Element> hotkeys = new ArrayList<>();

    hotkeys.add(text("[G] Go").fg(Color.GREEN).bold());
    hotkeys.add(text("[tab] Change focus").bold());
    if (isTenantListFocused(runner)) {
      hotkeys.add(text("[↑↓] Select tenant").bold());
    } else {
      hotkeys.add(text("[↑↓] Navigate").bold());
      hotkeys.add(text("[→←] Show/hide options").bold());
      hotkeys.add(text("[Space] Toggle").bold());
      hotkeys.add(text("[*] Toggle all").bold());
    }

    return hotkeys;
  }

  private static StyledElement<?> renderNode(TreeNode<TreeNodeJobReference> node) {
    TreeNodeJobReference ref = node.data();

    if (ref.property() != null) {
      return row(text(ref.getPrefix()), spacer(1), ref.property().getLabel()).style(ref.getStyle());
    } else {
      Row row = row(text(ref.getPrefix()), spacer(1), text(ref.job().name())).style(ref.getStyle());
      int unavailable = (int) ref.job().configuration().stream().filter(JobConfigurationProperty::isDisabled).count();
      if (unavailable == ref.job().configuration().size()) {
        row.add(text(" (unavailable)").dim());
      } else if (unavailable != 0) {
        row.add(text(" (" + unavailable + " unavailable)").dim());
      }
      row.add(text(" ".repeat(Math.max(1, ref.job().description().length() - ref.job().name().length() + 2))));

      if (node.isExpanded()) {
        return column(row, text(" " + ref.job().description()).italic()).style(ref.getStyle());
      } else {
        return row;
      }
    }
  }

  private boolean isTenantListFocused(ToolkitRunner runner) {
    return runner.focusManager().isFocused("job-configuration-tenants-panel");
  }

  private record TreeNodeJobReference(JobBuilder job, JobConfigurationProperty property) {
    public Style getStyle() {
      if (property != null) {
        if (property.isDisabled()) {
          return Style.EMPTY.dim();
        }
        if (Boolean.TRUE.equals(property.getBooleanValue())) {
          return Style.EMPTY.fg(Color.GREEN);
        }
        return Style.EMPTY.fg(Color.RED);
      } else {
        if (job.configuration().stream().allMatch(JobConfigurationProperty::isDisabled)) {
          return Style.EMPTY.dim();
        }
        // if all are ON or disabled
        if (job.configuration().stream().allMatch(p -> p.isOn() || p.isDisabled())) {
          return Style.EMPTY.fg(Color.GREEN);
        }
        // if any are ON specifically
        if (job.configuration().stream().anyMatch(JobConfigurationProperty::isOn)) {
          return Style.EMPTY.fg(Color.YELLOW);
        }
        return Style.EMPTY.fg(Color.RED);
      }
    }

    public String getPrefix() {
      if (property != null) {
        if (property.isDisabled()) {
          return "N/A";
        }
        if (Boolean.TRUE.equals(property.getBooleanValue())) {
          return "[x]";
        }
        return "[ ]";
      } else {
        if (job.configuration().stream().allMatch(JobConfigurationProperty::isDisabled)) {
          return "N/A";
        }
        // if all are ON or disabled
        if (job.configuration().stream().allMatch(p -> p.isOn() || p.isDisabled())) {
          return "[x]";
        }
        // if any are ON specifically
        if (job.configuration().stream().anyMatch(JobConfigurationProperty::isOn)) {
          return "[~]";
        }
        return "[ ]";
      }
    }

    public void toggle() {
      if (property != null) {
        if (property.isDisabled()) {
          return;
        }
        property.setBooleanValue(Boolean.FALSE.equals(property.getBooleanValue()));
      } else {
        // if all are ON or disabled
        if (job.configuration().stream().allMatch(p -> p.isOn() || p.isDisabled())) {
          job.configuration().stream().filter(p -> !p.isDisabled()).forEach(p -> p.setBooleanValue(false));
        } else {
          job.configuration().stream().filter(p -> !p.isDisabled()).forEach(p -> p.setBooleanValue(true));
        }
      }
    }
  }
}
