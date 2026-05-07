package org.folio.anonymization.jobs;

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

  @Autowired
  private SharedExecutionContext context;

  @Autowired
  private UtilRepository utilRepository;

  @Override
  public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
    return List.of(
      new JobBuilder(
        "Tenant metadata redaction",
        "Redacts information about the user who created/updated this tenant",
        tenant,
        context,
        JobConfigurationProperty.fromFieldList(FIELDS, tenant, utilRepository),
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
                    new GlobalFieldReference("public", "tenant_info", "tenant_id")
                      .field(null, String.class)
                      .eq(tenant.tenant().id())
                  )
                )
                .toList()
            )
      )
    );
  }
}
