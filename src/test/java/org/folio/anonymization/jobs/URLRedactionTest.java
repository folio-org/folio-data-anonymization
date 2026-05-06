package org.folio.anonymization.jobs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.folio.anonymization.domain.db.ModuleTable;
import org.folio.anonymization.domain.folio.Tenant;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobPart;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;

class URLRedactionTest {

  private static final Tenant TEST_TENANT = new Tenant("test", "Test", "Test tenant", null, false);

  @Test
  void buildSchedulesUrlRedactionParts() throws Exception {
    Job job = buildJobWithTables(
      new ModuleTable("agreements", "document_attachment", 100),
      new ModuleTable("agreements", "package_description_url", 100),
      new ModuleTable("agreements", "string_template", 100),
      new ModuleTable("copycat", "profile", 100),
      new ModuleTable("erm_usage", "usage_data_providers", 100),
      new ModuleTable("kb_ebsco_java", "kb_credentials", 100),
      new ModuleTable("licenses", "document_attachment", 100),
      new ModuleTable("organizations_storage", "contacts", 100),
      new ModuleTable("organizations_storage", "organizations", 100),
      new ModuleTable("organizations_storage", "interfaces", 100),
      new ModuleTable("organizations_storage", "privileged_contacts", 100)
    );
    List<? extends JobPart> parts = getParts(job, "redact");
    assertEquals(14, parts.size());

    assertHasLabel(parts, "agreements.document_attachment.da_url");
    assertHasLabel(parts, "agreements.package_description_url.pdu_url");
    assertHasLabel(parts, "agreements.string_template.strt_rule");
    assertHasLabel(parts, "copycat.profile.jsonb->'$.url'");
    assertHasLabel(parts, "erm_usage.usage_data_providers.jsonb->'$.harvestingConfig.sushiConfig.serviceUrl'");
    assertHasLabel(parts, "kb_ebsco_java.kb_credentials.url");
    assertHasLabel(parts, "licenses.document_attachment.da_url");
    assertHasLabel(parts, "organizations_storage.contacts.jsonb->'$.urls[*].value'");
    assertHasLabel(parts, "organizations_storage.organizations.jsonb->'$.contacts.urls[*].value'");
    assertHasLabel(parts, "organizations_storage.organizations.jsonb->'$.privilegedContacts.urls[*].value'");
    assertHasLabel(parts, "organizations_storage.interfaces.jsonb->'$.uri'");
    assertHasLabel(parts, "organizations_storage.organizations.jsonb->'$.urls[*].value'");
    assertHasLabel(parts, "organizations_storage.organizations.jsonb->'$.agreements[*].referenceUrl'");
    assertHasLabel(parts, "organizations_storage.privileged_contacts.jsonb->'$.urls[*].value'");
  }

  @Test
  void buildDoesNotSchedulePartsWhenTargetTablesAreMissing() throws Exception {
    Job job = buildJobWithTables(new ModuleTable("users", "outbox_event_log", 10));
    assertTrue(getParts(job, "redact").isEmpty());
  }

  private static Job buildJobWithTables(ModuleTable... tables) throws Exception {
    URLRedaction redaction = createFactoryWithContext();
    TenantExecutionContext tenant = new TenantExecutionContext(TEST_TENANT, List.of(tables));
    return redaction.getBuilders(tenant).getFirst().build();
  }

  private static URLRedaction createFactoryWithContext() throws Exception {
    URLRedaction redaction = new URLRedaction();
    Field contextField = URLRedaction.class.getDeclaredField("context");
    contextField.setAccessible(true);
    contextField.set(redaction, new SharedExecutionContext((DSLContext) null, Runnable::run));
    return redaction;
  }

  private static List<? extends JobPart> getParts(Job job, String phase) {
    ConcurrentLinkedQueue<?> partsForPhase = job.getParts().get(phase);
    if (partsForPhase == null) {
      return List.of();
    }
    List<? extends JobPart> parts = partsForPhase.stream().map(part -> (JobPart) part).toList();
    assertNotNull(parts);
    return parts;
  }

  private static void assertHasLabel(List<? extends JobPart> parts, String token) {
    assertTrue(parts.stream().anyMatch(part -> part.getLabel().contains(token)));
  }
}
