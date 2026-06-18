package org.folio.anonymization.domain.job;

import java.util.concurrent.Executor;
import org.jooq.DSLContext;

public record SharedExecutionContext(DSLContext create, DSLContext createKeycloak, Executor executor) {}
