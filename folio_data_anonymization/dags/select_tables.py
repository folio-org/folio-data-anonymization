import math
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
    from plugins.git_plugins.select_tables import (
        do_anonymize,
        fetch_number_of_records,
        schemas_tables,
    )
except (ImportError, ModuleNotFoundError):
    from folio_data_anonymization.plugins.select_tables import (
        do_anonymize,
        fetch_number_of_records,
        schemas_tables,
    )


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
            1000,
            type="integer",
            description="Number of table records to anonymize for a given run.",
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
    def do_batch_size() -> int:
        context = get_current_context()
        params = context.get("params", {})  # type: ignore
        batch = params["batch_size"]

        return int(batch)

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
        return schemas_tables(config)

    @task(map_index_template="{{ schema_name }}")
    def number_of_records(schema_table):
        context = get_current_context()
        context["schema_name"] = schema_table
        return fetch_number_of_records(schema=schema_table)

    @task
    def calculate_div(**kwargs):
        total = kwargs["number_of_records"]
        batch_size = kwargs["number_in_batch"]

        return math.ceil(total / batch_size)

    @task(multiple_outputs=True)
    def calculate_start_stop(**kwargs):
        output = {}
        div = kwargs["div"]
        batch_size = kwargs["batch_size"]
        total = kwargs["number_of_records"]

        output["start"] = int((div * batch_size) - batch_size + 1)
        stop = int(div * batch_size)
        output["stop"] = stop
        if stop > total:
            output["stop"] = total

        return output

    @task
    def anonymize_for_batch(div, start, stop):
        do_anonymize(div, start, stop)

    batch_size = do_batch_size()

    configuration = fetch_configuration()

    schemas_tables_selected = select_schemas_tables(configuration)

    total_records_per_table = number_of_records.expand(
        schema_table=schemas_tables_selected
    )

    record_div = calculate_div(
        number_of_records=total_records_per_table,
        number_in_batch=batch_size,
    )

    start_stop = calculate_start_stop.partial(div=record_div).expand(
        batch_size=batch_size, number_of_records=total_records_per_table
    )

    anonymize = anonymize_for_batch.partial(div=record_div).expand_kwargs(start_stop)


(
    configuration
    >> schemas_tables_selected
    >> total_records_per_table
    >> anonymize
)
