package org.folio.anonymization.domain.noninteractive;

public record ConfigurationParameters(
  int allowedRetries,
  int threadPoolSize,
  int connectionPoolSize,
  int queryTimeoutDurationSeconds
) {
  public void validate() {
    if (this.allowedRetries <= 0) {
      throw new IllegalArgumentException("`allowedRetries` must be at least one.");
    }
    if (this.threadPoolSize <= 0) {
      throw new IllegalArgumentException("`threadPoolSize` must be at least one.");
    }
    if (this.connectionPoolSize <= 0) {
      throw new IllegalArgumentException("`connectionPoolSize` must be at least one.");
    }
    if (this.queryTimeoutDurationSeconds <= 0) {
      throw new IllegalArgumentException("`queryTimeoutDurationSeconds` must be at least one.");
    }
  }
}
