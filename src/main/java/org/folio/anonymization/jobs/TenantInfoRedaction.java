package org.folio.anonymization.jobs;

import static dev.tamboui.toolkit.Toolkit.row;
import static dev.tamboui.toolkit.Toolkit.spacer;
import static dev.tamboui.toolkit.Toolkit.text;

import java.util.List;
import org.folio.anonymization.domain.db.GlobalFieldReference;
import org.folio.anonymization.domain.job.Job;
import org.folio.anonymization.domain.job.JobBuilder;
import org.folio.anonymization.domain.job.JobConfigurationProperty;
import org.folio.anonymization.domain.job.JobFactory;
import org.folio.anonymization.domain.job.SharedExecutionContext;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.jobs.templates.RedactPart;
import org.folio.anonymization.repository.UtilRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TenantInfoRedaction implements JobFactory {

  private static final List<GlobalFieldReference> FIELDS = List.of(
    new GlobalFieldReference("public", "tenant_info", "created_by_username"),
    new GlobalFieldReference("public", "tenant_info", "updated_by_username")
  );

  private static final GlobalFieldReference TENANT_ID_FIELD = new GlobalFieldReference(
    "public",
    "tenant_info",
    "tenant_id"
  );

  @Autowired
  private SharedExecutionContext context;

  @Autowired
  private UtilRepository utilRepository;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    boolean tenantInfoTableExists = utilRepository.doesTableExist(TENANT_ID_FIELD.schema(), TENANT_ID_FIELD.table());
    boolean tenantIsInTable =
      tenantInfoTableExists &&
      context
        .create()
        .fetchExists(
          context
            .create()
            .selectOne()
            .from(TENANT_ID_FIELD.table())
            .where(TENANT_ID_FIELD.field(null, String.class).eq(tenant.tenant().id()))
        );

    return List.of(
      new JobBuilder(
        "tenant_metadata",
        "Tenant metadata redaction",
        "Redacts information about the user who created/updated this tenant",
        tenant,
        context,
        FIELDS
          .stream()
          .map(field -> {
            if (!tenantIsInTable) {
              return new JobConfigurationProperty(
                field,
                row(
                  text(field.toString()).crossedOut(),
                  spacer(1),
                  text(tenantInfoTableExists ? "(not present for tenant)" : "(not available)").italic()
                ),
                true,
                true
              );
            } else {
              return new JobConfigurationProperty(field, text(field.toString()), true, false);
            }
          })
          .toList(),
        ctx ->
          new Job(ctx, List.of("redact"))
            // we will only update one row so no need to batch
            .scheduleParts(
              "redact",
              JobConfigurationProperty
                .getEnabledFields(ctx.settings())
                .map(GlobalFieldReference.class::cast)
                .map(field ->
                  new RedactPart(
                    "Redact " + field.toString(),
                    field,
                    TENANT_ID_FIELD.field(null, String.class).eq(tenant.tenant().id())
                  )
                )
                .toList()
            )
      )
    );
  }
}
