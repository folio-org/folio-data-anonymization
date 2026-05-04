package org.folio.anonymization.controller;

import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.tuple.Pair;
import org.folio.anonymization.domain.folio.Tenant;
import org.folio.anonymization.domain.job.JobBuilder;
import org.springframework.stereotype.Component;

@Log4j2
@Getter
@ToString
@EqualsAndHashCode
@Component
public class TUIState {

  public static enum State {
    INIT, // just launched, will be loading tenants...
    TENANT_SELECTION, // waiting on user to pick tenants
    JOB_LOADING, // we are fetching all the table sizes/etc to see what jobs are available
    JOB_CONFIGURATION, // pick settings
    JOB_EXECUTION, // progress tracker mode
    END, // all done! report/summary of what was skipped or any other pertinent info

    QUIT_CONFIRMATION,
    SHUTTING_DOWN, // nice message to leave on while Spring/Hikari close everything, triggers the actual .quit on SECOND render
  }

  private State state = State.INIT;
  private State stateBeforeQuitConfirmation = null;

  // from LoadingTenantsView
  private Map<String, Tenant> allTenants;

  // from TenantSelectionView
  private List<Tenant> selectedTenants;

  // from LoadingJobsView
  private List<Pair<Tenant, List<JobBuilder>>> availableJobs;

  // for ShutdownView
  @Setter
  private boolean readyToQuit = false;

  public void completeLoadingTenants(Map<String, Tenant> tenants) {
    this.allTenants = tenants;
    this.state = State.TENANT_SELECTION;

    log.info("Completed loading tenants! Moving to TENANT_SELECTION...");
  }

  public void selectTenants(List<Tenant> tenants) {
    this.selectedTenants = tenants.stream().sorted().toList();
    this.state = State.JOB_LOADING;

    log.info("Tenants were selected! Moving to JOB_LOADING...");
  }

  public void completeLoadingJobs(List<Pair<Tenant, List<JobBuilder>>> availableJobs) {
    this.availableJobs = availableJobs;
    this.state = State.JOB_CONFIGURATION;

    log.info("Completed loading jobs! Moving to JOB_CONFIGURATION...");
  }

  public void attemptToQuit() {
    if (state == State.QUIT_CONFIRMATION) {
      return;
    }

    this.stateBeforeQuitConfirmation = this.state;
    this.state = State.QUIT_CONFIRMATION;
  }

  public void confirmQuitAttempt() {
    if (state != State.QUIT_CONFIRMATION) {
      return;
    }

    this.state = State.SHUTTING_DOWN;
  }

  public void cancelQuitAttempt() {
    if (state != State.QUIT_CONFIRMATION) {
      return;
    }

    this.state = this.stateBeforeQuitConfirmation;
    this.stateBeforeQuitConfirmation = null;
  }
}
