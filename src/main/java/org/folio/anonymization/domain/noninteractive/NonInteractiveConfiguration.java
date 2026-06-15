package org.folio.anonymization.domain.noninteractive;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record NonInteractiveConfiguration(
  ConfigurationParameters parameters,
  List<Pattern> tenants,
  Map<String, JobConfigurationNonInteractive> jobs
) {
  public void validate() {
    if (this.parameters() == null) {
      throw new IllegalArgumentException("`parameters` must be provided.");
    }
    if (this.tenants() == null || this.tenants().isEmpty()) {
      throw new IllegalArgumentException("`tenants` must be provided and non-empty.");
    }
    if (this.jobs() == null || this.jobs().isEmpty()) {
      throw new IllegalArgumentException("`jobs` must be provided and non-empty.");
    }

    this.parameters().validate();
    this.jobs().entrySet().forEach(e -> e.getValue().validate(e.getKey(), e.getKey().equals("global-configuration")));
  }
}
