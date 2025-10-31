import logging
import math
import pathlib

from datetime import timedelta

from airflow.sdk import DAG, Param, get_current_context, task


logger = logging.getLogger(__name__)


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
        fetch_record_counts_per_table,
        schemas_tables,
    )
except (ImportError, ModuleNotFoundError):
    from folio_data_anonymization.plugins.select_tables import (
        do_anonymize,
        fetch_record_counts_per_table,
        schemas_tables,
    )


default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=30),
}


def config_file_names() -> list:
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
    schedule=None,
    default_args=default_args,
    catchup=False,
    tags=["select"],
    params={
        "batch_size": Param(
            250,
            type="integer",
            description="Number of table records to anonymize for a given run.",
        ),
        "configuration_files": Param(
            "Choose a configuration",
            type="string",
            description="Choose one of the configurations.",
            enum=config_file_names(),
        ),
        "tenant_id": Param(
            "diku",
            description="Tenant ID",
            type="string",
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
    def fetch_configuration() -> dict:
        context = get_current_context()
        params = context.get("params", {})  # type: ignore
        config_file = params.get("configuration_files", "")
        return configurations(
            pathlib.Path("/opt/bitnami/airflow"),
            pathlib.Path("plugins/git_plugins/config"),
            config_file,
        )

    @task
    def select_schemas_tables(config) -> list:
        context = get_current_context()
        params = context.get("params", {})  # type: ignore
        tenant_id = params["tenant_id"]
        return schemas_tables(config, tenant_id)

    @task(map_index_template="{{ schema_name }}")
    def record_counts_per_table(schema_table) -> int:
        context = get_current_context()
        context["schema_name"] = schema_table  # type: ignore
        return fetch_record_counts_per_table(schema=schema_table)

    @task
    def combine_table_counts(**kwargs) -> dict:
        totals = kwargs["record_counts_per_table"]
        tables = kwargs["schema_table"]
        list_totals = list(totals)

        return dict(zip(tables, list_totals))

    @task
    def calculate_table_ranges(**kwargs) -> list:
        output = []
        counts = kwargs["counts"]
        batch_size = kwargs["batch_size"]
        tables = kwargs["schemas_tables"]

        for table in tables:
            payload = {}
            payload["table"] = table
            payload["ranges"] = []
            records_in_table = counts[table]
            div = math.ceil(int(records_in_table) / int(batch_size))
            step = math.ceil(records_in_table / div)
            for i in range(0, records_in_table, step):
                payload["ranges"].append((i, i + step))

            output.append(payload)

        return output

    @task
    def anonymize_batches(table_ranges, configuration) -> None:
        context = get_current_context()
        params = context.get("params", {})  # type: ignore
        tenant_id = params["tenant_id"]

        do_anonymize(table_ranges, configuration, tenant_id)

    batch_size = do_batch_size()

    configuration = fetch_configuration()

    schemas_tables_selected = select_schemas_tables(configuration)

    total_records_per_table = record_counts_per_table.expand(
        schema_table=schemas_tables_selected
    )

    record_counts = combine_table_counts(
        number_in_batch=batch_size,
        schema_table=schemas_tables_selected,
        record_counts_per_table=total_records_per_table,
    )

    table_ranges = calculate_table_ranges(
        batch_size=batch_size,
        schemas_tables=schemas_tables_selected,
        counts=record_counts,
    )

    anonymize = anonymize_batches(table_ranges, configuration)


(
    configuration
    >> schemas_tables_selected
    >> total_records_per_table
    >> record_counts
    >> table_ranges
    >> anonymize
)
