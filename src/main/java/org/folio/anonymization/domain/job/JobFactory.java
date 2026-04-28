package org.folio.anonymization.domain.job;

import java.util.List;

/**
 * Represents single or multiple potentially available jobs. This class is responsible for
 * determining what job(s) apply based on the context, creating their builders, and passing
 * them along. This abstraction layer is primarily just to provide nice convenience for semantic
 * groups.
 *
 * Job implementors should use this for their instantiation. Be sure to annotate these with @Component!
 */
public interface JobFactory {
  public List<JobBuilder> getBuilders(TenantExecutionContext context);
}
