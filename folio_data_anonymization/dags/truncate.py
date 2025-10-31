from datetime import timedelta

from airflow.sdk import DAG, task

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

with DAG(
    "truncate_tables",
    schedule=None,
    default_args=default_args,
    catchup=False,
    tags=["truncate"],
    params={},
) as dag:

    @task
    def fetch_schemas_tables():
        return tables_list()

    @task
    def truncate_schemas_tables(schemas_tables):
        return truncate_db_objects(schemas_tables)

    schemas_tables = fetch_schemas_tables()

    truncate_database_objects = truncate_schemas_tables(schemas_tables)


(schemas_tables >> truncate_database_objects)
