package org.folio.anonymization.domain.job;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.tuple.Pair;

@Data
@Log4j2
public final class Job {

  private final String name;
  private final String description;
  private final JobContext context;

  /** Stages of the job; each will be executed in sequence */
  private final List<String> stages;
  private int currentStageIndex = 0;

  /**
   * Parts of the job, split as logically necessary (and/or based on application configuration).
   * Earlier parts (based on {@link #stages}) may alter parts belonging to future stages (for example,
   * one early part to scan a database may add a list of result stages).
   */
  private final ConcurrentMap<String, ConcurrentLinkedQueue<JobPart>> parts = new ConcurrentHashMap<>();

  /** State shared across job parts; recommended for inter-stage communication. */
  private final ConcurrentMap<String, AtomicReference<Object>> state = new ConcurrentHashMap<>();

  private final ConcurrentMap<String, Pair<JobPart, CompletableFuture<JobPart>>> currentlyExecuting = new ConcurrentHashMap<>();

  public Job(JobContext context) {
    this(context, List.of("read", "compute-values", "write"));
  }

  public Job(JobContext context, List<String> stages) {
    this.name = context.name();
    this.description = context.description();
    this.context = context;
    this.stages = stages;
  }

  public void execute() {
    this.executeNextStage();
  }

  protected void executeNextStage() {
    if (currentStageIndex >= stages.size()) {
      log.info("Job '{}' is complete.", name);
      return;
    }

    String stage = stages.get(currentStageIndex);
    ConcurrentLinkedQueue<JobPart> stageParts = this.parts.getOrDefault(stage, new ConcurrentLinkedQueue<>());
    log.info("Job '{}': starting stage '{}' of {} parts", name, stage, stageParts.size());

    this.currentlyExecuting.clear();
    stageParts
      .stream()
      .forEach(part -> {
        part.setStage(stage);
        this.executePart(part);
      });

    // in case there are no parts in the stage
    this.checkNextStageEligibility();
  }

  // declared as public to permit higher level interface to "re-execute" parts if needed
  public void executePart(JobPart part) {
    part.setJob(this);
    CompletableFuture<JobPart> future = CompletableFuture.supplyAsync(part, this.context.executionContext().executor());

    if (this.currentlyExecuting.put(part.getLabel(), Pair.of(part, future)) != null) {
      log.throwing(
        new IllegalStateException("Job '%s': part '%s' was found more than once".formatted(name, part.getLabel()))
      );
    }

    future.handle((r, e) -> {
      if (e == null) {
        log.info("Job '{}': completed part '{}'.", name, r.getLabel());
        this.currentlyExecuting.remove(r.getLabel());
      } else {
        log.error("Job '{}': error in part '{}'.", name, r.getLabel(), e);
      }
      this.checkNextStageEligibility();
      this.context.executionContext().jobNotifier().onStatusUpdate(this);
      return null;
    });
    this.context.executionContext().jobNotifier().onStatusUpdate(this);
  }

  /**
   * Should only be used for error recovery where a part will simply not work
   * and is preventing further execution. Most cases the whole Job should probably
   * be killed, but providing this API just in case.
   */
  public void skipPart(JobPart part) {
    this.currentlyExecuting.remove(part.getLabel());
    this.checkNextStageEligibility();
    this.context.executionContext().jobNotifier().onStatusUpdate(this);
  }

  protected void checkNextStageEligibility() {
    synchronized (currentlyExecuting) {
      if (currentlyExecuting.isEmpty()) {
        log.info("Job '{}' completed stage '{}'.", name, stages.get(currentStageIndex));
        currentStageIndex++;
        this.executeNextStage();
      }
    }
    this.context.executionContext().jobNotifier().onStatusUpdate(this);
  }

  public synchronized Job scheduleParts(String destinationStage, List<? extends JobPart> jobParts) {
    if (!stages.contains(destinationStage)) {
      throw new IllegalArgumentException("Job '%s': stage '%s' is not recognized".formatted(name, destinationStage));
    }
    parts.computeIfAbsent(destinationStage, k -> new ConcurrentLinkedQueue<>()).addAll(jobParts);
    return this;
  }

  public boolean isDone() {
    return this.currentStageIndex >= this.stages.size();
  }
}
