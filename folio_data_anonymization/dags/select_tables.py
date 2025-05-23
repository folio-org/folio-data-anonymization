import pathlib

from datetime import timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.python import get_current_context


try:
    from plugins.git_plugins.configurations import (
        configuration_files,
        configurations,
    )
except (ImportError, ModuleNotFoundError):
    from folio_data_anonymization.plugins.configurations import (
        configuration_files,
        configurations,
    )

try:
    from plugins.git_plugins.select_tables import fetch_number_of_records
except (ImportError, ModuleNotFoundError):
    from folio_data_anonymization.plugins.select_tables import fetch_number_of_records


default_args = {
    "owner": "libsys",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=30),
}


def config_file_names():
    config_names = [
        str(path.name)
        for path in configuration_files(
            pathlib.Path("/opt/bitnami/airflow"),
            "plugins/git_plugins/config",
        )
    ]
    config_names.insert(0, "Choose a configuration")
    return config_names


with DAG(
    "select_table_objects",
    default_args=default_args,
    catchup=False,
    tags=["select"],
    params={
        "batch_size": Param(
            5000,
            type="integer",
            description="Number of MARC records to process per file.",
        ),
        "concurrent_jobs": Param(
            5,
            type="integer",
            description="Number of batch processing jobs to run in parallel.",
        ),
        "configuration_files": Param(
            "Choose a configuration",
            type="string",
            description="Choose one of the configurations.",
            enum=config_file_names(),
        ),
    },
) as dag:

    @task
    def fetch_configuration():
        context = get_current_context()
        params = context.get("params", {})  # type: ignore
        config_file = params.get("configuration_files", "")
        return configurations(
            pathlib.Path("/opt/bitnami/airflow"),
            pathlib.Path("plugins/git_plugins/config"),
            config_file,
        )

    @task
    def select_schemas_tables(config):
        schemas_tables = []
        config_key = list(config.keys())[0]
        for table in config[config_key]:
            schemas_tables.append(table["table_name"])

        return schemas_tables

    @task(map_index_template="{{ schema_name }}")
    def number_of_records(schema_table):
        context = get_current_context()
        context["schema_name"] = schema_table
        return fetch_number_of_records(schema=schema_table)

    configuration = fetch_configuration()

    schemas_tables = select_schemas_tables(configuration)

    total_records = number_of_records.expand(schema_table=schemas_tables)


(configuration >> schemas_tables >> total_records)
