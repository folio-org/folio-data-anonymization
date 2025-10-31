from datetime import timedelta

from airflow.sdk import dag, task, Param

try:
    from plugins.git_plugins.truncate.truncate import (
        tables_list,
        truncate_db_objects,
    )
except (ImportError, ModuleNotFoundError):
    from folio_data_anonymization.plugins.truncate.truncate import (
        tables_list,
        truncate_db_objects,
    )


default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

@dag(
    schedule=None,
    default_args=default_args,
    catchup=False,
    tags=["truncate"],
    params={
        "tenant_id": Param(
            "diku",
            description="Tenant ID",
            type="string",
        ),
    },
)
def truncate_tables():

    @task
    def fetch_schemas_tables():
        return tables_list()

    @task
    def truncate_schemas_tables(schemas_tables):
        return truncate_db_objects(schemas_tables)

    schemas_tables = fetch_schemas_tables()

    truncate_schemas_tables(schemas_tables)


truncate_tables()
