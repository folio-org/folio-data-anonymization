package org.folio.anonymization.domain.job;

public interface JobNotifier {
  public void onStatusUpdate(Job job);
}
