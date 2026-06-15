package org.folio.anonymization.controller;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.zaxxer.hikari.HikariDataSource;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.tuple.Pair;
import org.folio.anonymization.domain.folio.Tenant;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.JobPart;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.domain.noninteractive.JobConfigurationNonInteractive;
import org.folio.anonymization.domain.noninteractive.NonInteractiveConfiguration;
import org.folio.anonymization.repository.TenantRepository;
import org.jooq.DSLContext;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Controller;
import tools.jackson.core.json.JsonReadFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.dataformat.yaml.YAMLMapper;

@Log4j2
@Controller
@RequiredArgsConstructor
public class NonInteractiveController {

  private static final ObjectMapper OBJECT_MAPPER = JsonMapper
    .builder()
    .enable(SerializationFeature.INDENT_OUTPUT)
    .changeDefaultPropertyInclusion(incl -> incl.withContentInclusion(JsonInclude.Include.NON_NULL))
    .build();

  private final ApplicationContext ctx;
  private final DSLContext create;
  private final HikariDataSource dataSource;
  private final ThreadPoolExecutor executor;

  private final List<JobFactory> jobFactories;
  private final TenantRepository tenantRepository;

  private NonInteractiveConfiguration configuration;

  public void setConfigurationFromPath(String path) {
    log.info("Setting configuration from path: {}", path);

    try {
      String fileContent = new String(Files.readAllBytes(Paths.get(path)));
      ObjectMapper objectMapper;
      if (fileContent.trim().startsWith("{")) {
        log.info("Detected JSON format for configuration file; parsing as JSON...");
        objectMapper =
          JsonMapper
            .builder()
            // allows use of Java/C++ style comments (both '/'+'*' and '//' varieties) within parsed content.
            .enable(JsonReadFeature.ALLOW_JAVA_COMMENTS)
            // some SQL statements may be cleaner this way around
            .enable(JsonReadFeature.ALLOW_SINGLE_QUOTES)
            // left side of { foo: bar }, cleaner/easier to read. JS style
            .enable(JsonReadFeature.ALLOW_UNQUOTED_PROPERTY_NAMES)
            // nicer diffs/etc
            .enable(JsonReadFeature.ALLOW_TRAILING_COMMA)
            // allows "escaping" newlines in regular JSON, giving proper linebreaks
            .enable(JsonReadFeature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER)
            .build();
      } else {
        log.info("Detected YAML format for configuration file; parsing as YAML...");
        objectMapper = YAMLMapper.builder().build();
      }

      NonInteractiveConfiguration newConfig = objectMapper.readValue(new File(path), NonInteractiveConfiguration.class);
      log.info("Read configuration from {}; validating...", path);
      newConfig.validate();
      this.configuration = newConfig;
      log.info("Read configuration from file successfully: {}", this.configuration);
    } catch (Exception e) {
      throw log.throwing(new RuntimeException("Failed to read configuration from file: " + e.getMessage(), e));
    }
  }

  public void run() {
    if (this.configuration == null) {
      throw log.throwing(new IllegalStateException("Cannot run non-interactive anonymization without configuration!"));
    }

    // do this early to prevent failing at the end
    try {
      Files.createDirectories(Paths.get("out"));
    } catch (Exception e) {
      throw log.throwing(new RuntimeException("Failed to create output directory for reports!", e));
    }

    log.info("Starting non-interactive anonymization with configuration: {}", this.configuration);
    log.info("Setting global parameters...");
    this.setGlobalParameters();

    log.info("Fetching tenants...");
    Map<String, Tenant> allTenants = tenantRepository.getAllTenants();
    List<Tenant> selectedTenants = allTenants
      .entrySet()
      .stream()
      .filter(entry ->
        this.configuration.tenants().stream().anyMatch(pattern -> pattern.matcher(entry.getKey()).matches())
      )
      .map(Map.Entry::getValue)
      .toList();

    log.info(
      "Found {} tenants, of which the following tenant(s) ({}) were matched: {}",
      allTenants.size(),
      selectedTenants.size(),
      selectedTenants
    );
    this.configuration.tenants()
      .stream()
      .filter(pattern -> selectedTenants.stream().noneMatch(tenant -> pattern.matcher(tenant.id()).matches()))
      .forEach(p -> log.warn("Pattern {} did not match any tenants in the environment!", p.pattern()));

    if (selectedTenants.isEmpty()) {
      throw log.throwing(new IllegalStateException("No tenants matched any of the provided patterns!"));
    }

    List<JobBuilder> availableJobBuilders = selectedTenants
      .stream()
      .flatMap(tenant -> {
        log.info("Loading jobs for tenant {}...", tenant.id());
        TenantExecutionContext context = tenantRepository.getTenantExecutionContext(tenant);
        return jobFactories.stream().map(factory -> factory.getBuilders(context));
      })
      .flatMap(List::stream)
      .toList();

    log.info("Found {} total available jobs", availableJobBuilders.size());

    log.info("Applying configuration options to available jobs...");
    availableJobBuilders =
      availableJobBuilders
        .stream()
        .map(builder ->
          this.configuration.jobs()
            .computeIfAbsent(
              builder.key(),
              k -> {
                throw log.throwing(
                  new IllegalArgumentException(
                    "A job with key `" +
                    builder.key() +
                    "` is available for " +
                    builder.tenant().tenant().id() +
                    ", however, it was not found in the configuration!"
                  )
                );
              }
            )
            .apply(builder)
        )
        .toList();

    List<JobBuilder> skippedJobBuilders = availableJobBuilders
      .stream()
      .filter(builder -> builder.configuration().stream().noneMatch(JobConfigurationProperty::isOn))
      .toList();

    List<JobBuilder> jobBuilders = availableJobBuilders
      .stream()
      .filter(builder -> builder.configuration().stream().anyMatch(JobConfigurationProperty::isOn))
      .toList();

    log.info("{} job(s) remain after applying configuration options.", jobBuilders.size());

    log.info("Starting jobs...");

    List<Job> jobs = new ArrayList<>(jobBuilders.size());
    for (int i = 0; i < jobBuilders.size(); i++) {
      Job job = jobBuilders.get(i).build();

      job.execute();
      jobs.add(job);

      log.info("Started job {}/{}: {}", i + 1, jobBuilders.size(), job.getName());

      try {
        // be a little gentle to not completely start slamming the DB
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    log.info("All jobs started successfully! Monitoring for completion...");

    Map<Tenant, Map<Job, List<Pair<JobPart, Throwable>>>> failedParts = new HashMap<>();

    do {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }

      for (Job job : jobs) {
        job
          .getFailedParts()
          .forEach(part -> {
            log.error(
              "Job {} has part {} that failed with exception: {}",
              job.getName(),
              part.getLeft().getLabel(),
              part.getRight().getMessage(),
              part.getRight()
            );
            failedParts
              .computeIfAbsent(job.getContext().tenant().tenant(), t -> new HashMap<>())
              .computeIfAbsent(job, j -> new ArrayList<>())
              .add(part);
            job.disableTeardown();
            job.skipPart(part.getLeft());
          });
      }
    } while (!jobs.stream().allMatch(Job::isCompleted));

    log.info("All jobs completed!");
    log.info("Creating reports...");

    selectedTenants.forEach(tenant -> {
      boolean hadFailures = failedParts.containsKey(tenant);
      try {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();

        map.put("tenantId", tenant.id());
        map.put("hadFailures", hadFailures);
        map.put("completedAt", Instant.now().toString());
        map.put("configuration", this.configuration); // for reference
        map.put(
          "disabledJobs",
          skippedJobBuilders
            .stream()
            .filter(b -> b.tenant().tenant().id().equals(tenant.id()))
            .map(b -> Map.entry(b.key(), getJsonRepresentation(b)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new))
        );
        map.put(
          "failedParts",
          failedParts
            .getOrDefault(tenant, Map.of())
            .entrySet()
            .stream()
            .map(e ->
              Map.entry(
                e.getKey().getKey(),
                e
                  .getValue()
                  .stream()
                  .map(part -> Map.entry(part.getLeft().getLabel(), getJsonRepresentation(part.getRight())))
                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new))
              )
            )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new))
        );
        map.put(
          "executedJobs",
          jobs
            .stream()
            .filter(j -> j.getContext().tenant().tenant().id().equals(tenant.id()))
            .map(j -> Map.entry(j.getKey(), getJsonRepresentation(j)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new))
        );

        Files.write(Paths.get("out", tenant.id() + "-report.json"), OBJECT_MAPPER.writeValueAsBytes(map));
        log.info("Saved report for tenant {}", tenant.id());
      } catch (IOException e) {
        throw log.throwing(new UncheckedIOException(e));
      }

      if (hadFailures) {
        log.warn(
          "Tenant {} had failures during anonymization! Please review the generated report at {} for details and re-run the errored jobs.",
          tenant.id(),
          Paths.get("out", tenant.id() + "-report.json").toAbsolutePath()
        );

        Map<String, JobConfigurationNonInteractive> rerunJobConfiguration =
          this.configuration.jobs()
            .keySet()
            .stream()
            .map(k -> Map.entry(k, JobConfigurationNonInteractive.builder().enabled(false).build()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new));

        failedParts
          .getOrDefault(tenant, Map.of())
          .entrySet()
          .stream()
          .forEach(job ->
            rerunJobConfiguration.put(
              job.getKey().getKey(),
              JobConfigurationNonInteractive
                .builder()
                .enabled(true)
                .performSetup(false)
                .performTeardown(true)
                .removeUnknownModConfigurationEntries(
                  this.configuration.jobs().get(job.getKey().getKey()).removeUnknownModConfigurationEntries()
                )
                .removeUnknownModSettingEntries(
                  this.configuration.jobs().get(job.getKey().getKey()).removeUnknownModSettingEntries()
                )
                .build()
            )
          );

        try {
          Files.write(
            Paths.get("out", tenant.id() + "-rerun.json"),
            OBJECT_MAPPER.writeValueAsBytes(
              new NonInteractiveConfiguration(
                this.configuration.parameters(),
                List.of(Pattern.compile("^" + Pattern.quote(tenant.id()) + "$")),
                rerunJobConfiguration
              )
            )
          );
          log.info("Saved re-run configuration for tenant {}", tenant.id());
        } catch (IOException e) {
          throw log.throwing(new UncheckedIOException(e));
        }
      }
    });

    log.info("All done! Shutting down...");
    this.executor.shutdown();
    SpringApplication.exit(ctx, () -> 0);
  }

  protected Map<String, String> getJsonRepresentation(Throwable throwable) {
    LinkedHashMap<String, String> map = new LinkedHashMap<>();

    map.put("type", throwable.getClass().getName());
    map.put("message", throwable.getMessage());
    map.put(
      "trace",
      Pattern
        .compile("(?<=\n)at .*")
        .matcher(
          throwable.toString() +
          "\n" +
          Arrays.stream(throwable.getStackTrace()).map(StackTraceElement::toString).collect(Collectors.joining("\n"))
        )
        .results()
        .map(m -> m.group().trim())
        .toList()
        .toString()
    );
    return map;
  }

  protected Map<String, String> getJsonRepresentation(JobBuilder jobBuilder) {
    return jobBuilder
      .configuration()
      .stream()
      .map(this::getJsonRepresentation)
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new));
  }

  protected Map.Entry<String, String> getJsonRepresentation(JobConfigurationProperty prop) {
    if (prop.isDisabled()) {
      return Map.entry(prop.getKey().toString(), "unavailable");
    } else if (prop.isOn()) {
      return Map.entry(prop.getKey().toString(), "on");
    } else {
      return Map.entry(prop.getKey().toString(), "off");
    }
  }

  protected Map.Entry<String, String> getJsonRepresentation(JobPart part) {
    if (part.getCompleted().get()) {
      return Map.entry(part.getLabel(), "completed");
    } else {
      return Map.entry(part.getLabel(), "failed");
    }
  }

  protected Map<String, Object> getJsonRepresentation(Job job) {
    Map<String, Object> map = new LinkedHashMap<>();

    map.put("name", job.getName());
    map.put("description", job.getDescription());
    map.put(
      "configuration",
      job
        .getContext()
        .settings()
        .stream()
        .map(this::getJsonRepresentation)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new))
    );
    map.put(
      "partsRan",
      job
        .getParts()
        .entrySet()
        .stream()
        .map(stage -> Map.entry(stage.getKey(), stage.getValue().stream().map(this::getJsonRepresentation).toList()))
        .sorted((a, b) -> job.getStages().indexOf(a.getKey()) - job.getStages().indexOf(b.getKey())) // preserve stage order
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new))
    );

    return map;
  }

  protected void setGlobalParameters() {
    log.info("Setting allowedRetries={}", this.configuration.parameters().allowedRetries());
    JobPart.setMaximumRetries(this.configuration.parameters().allowedRetries());

    log.info("Setting threadPoolSize={}", this.configuration.parameters().threadPoolSize());
    if (this.configuration.parameters().threadPoolSize() > this.executor.getMaximumPoolSize()) {
      // expanding pool
      this.executor.setMaximumPoolSize(this.configuration.parameters().threadPoolSize());
      this.executor.setCorePoolSize(this.configuration.parameters().threadPoolSize());
    } else {
      // shrinking pool
      this.executor.setCorePoolSize(this.configuration.parameters().threadPoolSize());
      this.executor.setMaximumPoolSize(this.configuration.parameters().threadPoolSize());
    }

    log.info("Setting connectionPoolSize={}", this.configuration.parameters().connectionPoolSize());
    this.dataSource.setMaximumPoolSize(this.configuration.parameters().connectionPoolSize());

    log.info("Setting queryTimeoutDurationSeconds={}", this.configuration.parameters().queryTimeoutDurationSeconds());
    create.configuration().settings().setQueryTimeout(this.configuration.parameters().queryTimeoutDurationSeconds());
  }
}
