package org.folio.anonymization.domain.job;

import java.util.List;
import lombok.With;
import org.folio.anonymization.domain.db.ModuleTable;
import org.folio.anonymization.domain.folio.Tenant;

/** Information about the tenant which a job will execute in */
@With
public record TenantExecutionContext(Tenant tenant, List<ModuleTable> availableTables) {}
