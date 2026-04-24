package org.folio.anonymization.domain.db;

import lombok.With;

// schema should not have a tenant or _ prefix
@With
public record ModuleTable(String schema, String table, Integer size) {}
