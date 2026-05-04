package org.folio.anonymization.domain.job;

import java.util.List;
import java.util.function.Function;

public record JobBuilder(
  String name,
  String description,
  TenantExecutionContext tenant,
  SharedExecutionContext executionContext,
  List<JobConfigurationProperty> configuration,
  Function<JobContext, Job> creator
)
  implements Comparable<JobBuilder> {
  public Job build() {
    return this.creator()
      .apply(
        new JobContext(
          "[%s] %s".formatted(tenant.tenant().id(), name),
          description,
          tenant,
          executionContext,
          configuration
        )
      );
  }

  @Override
  public int compareTo(JobBuilder that) {
    return this.name.compareTo(that.name);
  }
}
