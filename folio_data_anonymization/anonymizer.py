import os

# here we declare the list of schemas to anonymize as asynchronous tasks
# configuration file allows operator to run specific anonymizations
# operator should be able to run all or pick/choose what to anonymize (maybe a schema got messed up and needs to be run again?)

tenant = os.getenv("TENANT")

# TODO: a query to information_schemas given tenant
schemas: list = ["mod_circulation_storage", "mod_organization_storage", "mod_users"]


def construct_schema_names(**kwargs) -> list:
    tenant = kwargs.get("tenant")
    schemas = kwargs.get("schemas")
    return [f"{tenant}_{x}" for x in schemas]


schema_names = construct_schema_names(tenant=tenant, schemas=schemas)
