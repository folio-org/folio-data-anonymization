package org.folio.anonymization.repository;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.log4j.Log4j2;
import org.folio.anonymization.domain.db.ModuleTable;
import org.folio.anonymization.domain.folio.Tenant;
import org.folio.anonymization.domain.job.TenantExecutionContext;
import org.folio.anonymization.util.DBUtils;
import org.jooq.DSLContext;
import org.jooq.Table;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Log4j2
@Repository
public class TenantRepository {

  // relational from Eureka
  private static final Table<?> TENANT_TABLE = table(name("public", "tenant"));
  // JSON blob from Okapi
  private static final Table<?> TENANTS_TABLE = table(name("public", "tenants"));
  private static final String MOD_CONSORTIA_KEYCLOAK_SCHEMA = "consortia_keycloak";

  @Autowired
  private DSLContext create;

  @Autowired
  private UtilRepository utilRepository;

  public TenantExecutionContext getTenantExecutionContext(Tenant tenant, List<Tenant> consortiumSiblings) {
    return new TenantExecutionContext(tenant, this.getModuleTablesWithSizes(tenant.id()), consortiumSiblings);
  }

  public Map<String, Tenant> getAllTenants() {
    Map<String, Tenant> tenants = getTenantsWithoutConsortiaInfo();

    tenants
      .keySet()
      .stream()
      .map(this::getConsortiumInformationFromTenant)
      .flatMap(List::stream)
      .forEach(consortiumRecord -> {
        tenants.put(
          consortiumRecord.tenantId(),
          tenants
            .get(consortiumRecord.tenantId())
            .withName(consortiumRecord.tenantNiceName())
            .withConsortiumName(consortiumRecord.consortiumName())
            .withCentral(consortiumRecord.tenantIsCentral())
        );
      });

    return tenants;
  }

  protected Map<String, Tenant> getTenantsWithoutConsortiaInfo() {
    return Stream
      .concat(
        utilRepository.doesTableExist("public", "tenant")
          ? create
            .select(field("name"), field("description"))
            .from(TENANT_TABLE)
            .fetch()
            .stream()
            .map(record ->
              new Tenant(
                record.get("name", String.class),
                record.get("name", String.class),
                Objects.requireNonNullElse(record.get("description", String.class), ""),
                null,
                false
              )
            )
          : Stream.empty(),
        utilRepository.doesTableExist("public", "tenants")
          ? create
            .select(
              field("tenantjson->'descriptor'->>'id'").as("id"),
              field("tenantjson->'descriptor'->>'name'").as("name"),
              field("tenantjson->'descriptor'->>'description'").as("description")
            )
            .from(TENANTS_TABLE)
            .where(field("tenantjson->'descriptor'->>'id'").ne("supertenant"))
            .fetch()
            .stream()
            .map(record ->
              new Tenant(
                record.get("id", String.class),
                record.get("name", String.class),
                Objects.requireNonNullElse(record.get("description", String.class), ""),
                null,
                false
              )
            )
          : Stream.empty()
      )
      // tenants table has better data than tenant table, so use its data when possible
      .collect(Collectors.toMap(Tenant::id, Function.identity(), (a, b) -> b));
  }

  public List<ModuleTable> getModuleTablesWithSizes(String tenantId) {
    log.info("Getting all tables and sizes for tenant {}", tenantId);
    return utilRepository.getTablesSizesBySchemaPrefix(tenantId + "_mod_");
  }

  protected List<TenantConsortiumInfo> getConsortiumInformationFromTenant(String tenantName) {
    String schemaName = DBUtils.getSchemaName(tenantName, MOD_CONSORTIA_KEYCLOAK_SCHEMA);

    if (!utilRepository.doesSchemaExist(tenantName, MOD_CONSORTIA_KEYCLOAK_SCHEMA)) {
      return List.of();
    }

    return create
      .select(
        field(field("concat({0}, ' (', {1}, ')')", name("consortium", "name"), name("consortium", "id")))
          .as("consortium_name"),
        field(name("tenant", "id")).as("tenant_id"),
        field(name("tenant", "name")).as("tenant_name"),
        field(name("tenant", "is_central")).as("tenant_is_central")
      )
      .from(
        table(name(schemaName, "consortium"))
          .join(table(name(schemaName, "tenant")))
          .on(field(name("consortium", "id")).eq(field(name("tenant", "consortium_id"))))
      )
      .fetch()
      .stream()
      .map(row ->
        new TenantConsortiumInfo(
          row.get("tenant_id", String.class),
          row.get("tenant_name", String.class),
          row.get("consortium_name", String.class),
          row.get("tenant_is_central", Boolean.class)
        )
      )
      .toList();
  }

  public static Map<String, List<Tenant>> getConsortiumListFromTenants(Collection<Tenant> tenants) {
    return tenants
      .stream()
      .filter(t -> t.consortiumName() != null)
      .collect(Collectors.groupingBy(Tenant::consortiumName));
  }

  protected record TenantConsortiumInfo(
    String tenantId,
    String tenantNiceName,
    String consortiumName,
    boolean tenantIsCentral
  ) {}
}
