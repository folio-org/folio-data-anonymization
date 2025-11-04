import logging
import pathlib

from datetime import timedelta

from airflow.sdk import dag, Param, get_current_context, task


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
        construct_anon_config,
        combine_table_counts,
        calculate_table_ranges,
        fetch_records_batch_for_table,
        trigger_anonymize_dag,
        fetch_record_counts_per_table,
        schemas_tables,
    )
except (ImportError, ModuleNotFoundError):
    from folio_data_anonymization.plugins.select_tables import (
        construct_anon_config,
        combine_table_counts,
        calculate_table_ranges,
        fetch_records_batch_for_table,
        trigger_anonymize_dag,
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


@dag(
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
)
def select_table_objects(*args, **kwargs):

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
    def record_counts(batch_size, schemas_tables, total_records_per_table) -> dict:
        return combine_table_counts(
            number_in_batch=batch_size,
            schema_table=schemas_tables,
            record_counts_per_table=total_records_per_table,
        )

    @task
    def table_ranges(*args) -> list:
        return calculate_table_ranges(
            batch_size=batch_size,
            schemas_tables=schemas_tables,
            counts=record_counts,
        )

    @task
    def anonymize_batches(*args, **kwargs):
        """
        Creates batches to anonymize from list item of calculate_table_ranges
        {
            'table': 'diku_mod_organizations_storage.contacts',
            'ranges': [(0, 38)]
        }
        """
        context = get_current_context()
        params = context.get("params", {})  # type: ignore
        tenant_id = params["tenant_id"]
        table = kwargs["table_ranges"]["table"]
        logger.info(f"TABLE: {table}")
        config = construct_anon_config(kwargs["config"], table)
        logger.info(f"CONFIG: {config}")
        for range in kwargs["table_ranges"]["ranges"]:
            offset = range[0]
            limit = range[1] - offset

            data = fetch_records_batch_for_table(table, offset, limit)
            dag_run = trigger_anonymize_dag(data, config, tenant_id)
            dag_run.execute(context)

    batch_size = do_batch_size()

    configuration = fetch_configuration()

    schemas_tables_selected = select_schemas_tables(configuration)

    total_records_per_table = record_counts_per_table.expand(
        schema_table=schemas_tables_selected
    )

    counts = record_counts(batch_size, schemas_tables_selected, total_records_per_table)

    table_ranges(batch_size, schemas_tables_selected, counts)

    anonymize_batches.expand(table_ranges=table_ranges, config=configuration)


select_table_objects()
