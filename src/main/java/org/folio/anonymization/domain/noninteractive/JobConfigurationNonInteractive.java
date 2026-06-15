package org.folio.anonymization.domain.noninteractive;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import java.util.regex.Pattern;
import lombok.Builder;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.domain.job.JobBuilder;

@Log4j2
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public record JobConfigurationNonInteractive(
  boolean enabled,
  List<Pattern> whitelistOptions,
  List<Pattern> blacklistOptions,
  // assumed true if white/blacklist is not specified
  Boolean performSetup,
  Boolean performTeardown,
  // assumed false by default
  Boolean removeUnknownModConfigurationEntries,
  Boolean removeUnknownModSettingEntries
) {
  public void validate(String label, boolean isConfigurationOrSettingsJob) {
    if (
      this.enabled == false &&
      (
        this.whitelistOptions != null ||
        this.blacklistOptions != null ||
        this.performSetup != null ||
        this.performTeardown != null ||
        this.removeUnknownModConfigurationEntries != null ||
        this.removeUnknownModSettingEntries != null
      )
    ) {
      throw log.throwing(
        new IllegalArgumentException(
          "Job '" +
          label +
          "' is disabled but has additional options defined; please remove the options or set enabled=true."
        )
      );
    }

    if (this.whitelistOptions != null && this.blacklistOptions != null) {
      throw log.throwing(
        new IllegalArgumentException(
          "Job '" + label + "' has both whitelist and blacklist options defined; only one of these is allowed."
        )
      );
    }

    boolean hasWhitelistOrBlacklistOptions = this.whitelistOptions != null || this.blacklistOptions != null;

    if (this.performSetup != null && hasWhitelistOrBlacklistOptions) {
      throw log.throwing(
        new IllegalArgumentException(
          "Job '" +
          label +
          "' specifies performSetup but also has whitelist/blacklist options defined; please include the setup parts in the whitelist/blacklist."
        )
      );
    }

    if (this.performTeardown != null && hasWhitelistOrBlacklistOptions) {
      throw log.throwing(
        new IllegalArgumentException(
          "Job '" +
          label +
          "' specifies performTeardown but also has whitelist/blacklist options defined; please include the teardown parts in the whitelist/blacklist."
        )
      );
    }

    if (
      (this.removeUnknownModConfigurationEntries != null || this.removeUnknownModSettingEntries != null) &&
      !isConfigurationOrSettingsJob
    ) {
      throw log.throwing(
        new IllegalArgumentException(
          "Job '" +
          label +
          "' specifies removeUnknownModConfigurationEntries or removeUnknownModSettingEntries but is not a configuration or settings job; these options are only applicable for configuration or settings jobs."
        )
      );
    }
  }

  public JobBuilder apply(JobBuilder builder) {
    if (!this.enabled) {
      builder.configuration().stream().forEach(property -> property.setBooleanValue(false));
      return builder;
    }

    if (this.whitelistOptions != null) {
      builder
        .configuration()
        .stream()
        .filter(property -> !property.isDisabled())
        .forEach(property ->
          property.setBooleanValue(
            this.whitelistOptions.stream().anyMatch(pattern -> pattern.matcher(property.getKey().toString()).matches())
          )
        );
      return builder;
    } else if (this.blacklistOptions != null) {
      builder
        .configuration()
        .stream()
        .filter(property -> !property.isDisabled())
        .forEach(property ->
          property.setBooleanValue(
            this.blacklistOptions.stream().noneMatch(pattern -> pattern.matcher(property.getKey().toString()).matches())
          )
        );
      return builder;
    }

    // if enabled, we assume we want to do everything
    builder
      .configuration()
      .stream()
      .filter(property -> !property.isDisabled())
      .forEach(property -> property.setBooleanValue(true));

    builder
      .configuration()
      .stream()
      .filter(property -> !property.isDisabled() && property.getKey().equals("create-table"))
      .forEach(property -> property.setBooleanValue(!Boolean.FALSE.equals(this.performSetup)));
    builder
      .configuration()
      .stream()
      .filter(property -> !property.isDisabled() && property.getKey().equals("drop-table"))
      .forEach(property -> property.setBooleanValue(!Boolean.FALSE.equals(this.performTeardown)));

    // must opt into these
    builder
      .configuration()
      .stream()
      .filter(property ->
        !property.isDisabled() &&
        property.getKey() instanceof String &&
        ((String) property.getKey()).startsWith("mod-config-unknown-")
      )
      .forEach(property -> property.setBooleanValue(Boolean.TRUE.equals(this.removeUnknownModConfigurationEntries)));
    builder
      .configuration()
      .stream()
      .filter(property ->
        !property.isDisabled() &&
        property.getKey() instanceof String &&
        ((String) property.getKey()).startsWith("mod-settings-unknown-")
      )
      .forEach(property -> property.setBooleanValue(Boolean.TRUE.equals(this.removeUnknownModSettingEntries)));

    return builder;
  }
}
