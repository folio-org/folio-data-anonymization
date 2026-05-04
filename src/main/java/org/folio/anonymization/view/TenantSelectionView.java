package org.folio.anonymization.view;

import static dev.tamboui.toolkit.Toolkit.formField;
import static dev.tamboui.toolkit.Toolkit.panel;
import static dev.tamboui.toolkit.Toolkit.row;
import static dev.tamboui.toolkit.Toolkit.spacer;
import static dev.tamboui.toolkit.Toolkit.text;

import dev.tamboui.toolkit.element.Element;
import dev.tamboui.toolkit.element.StyledElement;
import dev.tamboui.toolkit.event.EventResult;
import dev.tamboui.widgets.form.BooleanFieldState;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.controller.TUIState;
import org.folio.anonymization.domain.folio.Tenant;
import org.springframework.stereotype.Component;

@Log4j2
@Component
@RequiredArgsConstructor
public class TenantSelectionView implements TUIView {

  private final TUIState state;

  private Map<String, BooleanFieldState> formState;
  private Element formElement;
  private Button submitButton;

  protected Element getForm() {
    if (this.formElement == null) {
      Map<String, Tenant> tenants = state.getAllTenants();

      this.formState =
        tenants.keySet().stream().collect(Collectors.toMap(Function.identity(), k -> new BooleanFieldState(true)));

      Map<String, List<Tenant>> tenantsGroupedByConsortium = new HashMap<>();
      tenants
        .values()
        .forEach(tenant -> {
          if (tenant.consortiumName() == null) {
            tenantsGroupedByConsortium.computeIfAbsent("Unaffiliated", k -> new ArrayList<>()).add(tenant);
          } else {
            tenantsGroupedByConsortium.computeIfAbsent(tenant.consortiumName(), k -> new ArrayList<>()).add(tenant);
          }
        });

      tenantsGroupedByConsortium
        .values()
        .forEach(consortia ->
          consortia.sort((a, b) -> {
            if (a.isCentral() && !b.isCentral()) {
              return -1;
            } else if (!a.isCentral() && b.isCentral()) {
              return 1;
            } else {
              return a.compareTo(b);
            }
          })
        );

      List<Element> formElements = new ArrayList<>();

      tenantsGroupedByConsortium
        .entrySet()
        .stream()
        .forEach(e -> {
          if (!formElements.isEmpty()) {
            formElements.add(spacer(1));
          }
          formElements.add(text(e.getKey()).bold());
          e
            .getValue()
            .forEach(tenant -> {
              StringBuilder label = new StringBuilder();
              label.append('[');
              label.append(tenant.id());
              label.append("]");
              if (tenant.isCentral()) {
                label.append(" [Central]");
              }
              // most tenants appear to have name=id
              if (!tenant.name().equals(tenant.id())) {
                label.append(' ');
                label.append(tenant.name());
              }
              if (!tenant.description().equals(tenant.id())) {
                label.append(' ');
                label.append(tenant.description());
              }
              formElements.add(
                row(
                  formField(label.toString(), this.formState.get(tenant.id()))
                    .inputBeforeLabel(true)
                    .labelWidth(label.length())
                    .length(label.length() + 4)
                )
              );
            });
        });

      this.submitButton =
        new Button(text(" Next"))
          .onPress(e -> {
            if (this.getSelectedTenants().isEmpty()) {
              log.warn("Tried selecting zero tenants!");
              return EventResult.UNHANDLED;
            }

            this.state.selectTenants(this.getSelectedTenants());
            return EventResult.HANDLED;
          });

      this.formElement =
        new Scrollable()
          .scrollUpIndicator(text("[scroll up to see more...]").dim())
          .scrollDownIndicator(text("[scroll down to see more...]").dim())
          .add(text("Please select all tenants to anonymize data for:"))
          .add(spacer(1))
          .add(formElements.toArray(new Element[0]))
          .add(spacer())
          .add(submitButton)
          .fill();
    }

    return this.formElement;
  }

  protected List<Tenant> getSelectedTenants() {
    if (this.formState == null) {
      return List.of();
    }
    return this.formState.entrySet()
      .stream()
      .filter(entry -> entry.getValue().value())
      .map(Map.Entry::getKey)
      .map(k -> state.getAllTenants().get(k))
      .toList();
  }

  @Override
  public StyledElement<?> render() {
    return panel("Tenant selection")
      .add(getForm())
      .rounded()
      .fill()
      .onKeyEvent(k -> {
        if (k.isChar('*')) {
          if (this.getSelectedTenants().isEmpty()) {
            this.formState.values().forEach(v -> v.setValue(true));
          } else {
            this.formState.values().forEach(v -> v.setValue(false));
          }
          return EventResult.HANDLED;
        }
        return EventResult.UNHANDLED;
      });
  }

  @Override
  public List<Element> getHotkeys() {
    Element actionHotkey = text("[Space] Toggle").bold();

    if (submitButton.isFocused()) {
      if (this.getSelectedTenants().isEmpty()) {
        actionHotkey = text("[Please select at least one tenant to continue]").red().italic();
      } else {
        actionHotkey = text("[Enter] Next").green().bold();
      }
    }

    return List.of(
      actionHotkey,
      text("[Tab or Mouse] Select next").bold(),
      text("[↑↓ or scroll] Scroll").bold(),
      this.getSelectedTenants().isEmpty() ? text("[*] Select all").bold() : text("[*] Deselect all").bold()
    );
  }
}
