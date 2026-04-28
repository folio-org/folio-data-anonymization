package org.folio.anonymization.domain.job;

import java.util.List;

public record JobContext(
  String name,
  String description,
  TenantExecutionContext tenant,
  SharedExecutionContext executionContext,
  List<JobConfigurationProperty> settings
) {}
