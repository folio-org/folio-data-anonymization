package org.folio.anonymization.domain.folio;

import lombok.With;

@With
public record Tenant(String id, String name, String description, String consortiumName, boolean isCentral) {}
