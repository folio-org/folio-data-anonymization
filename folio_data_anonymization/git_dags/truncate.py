from datetime import timedelta

from airflow import DAG
from airflow.decorators import task

from git_plugins.truncate import (
    tables_list,
    truncate_db_objects,
)


default_args = {
    "owner": "libsys",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "truncate_tables",
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
