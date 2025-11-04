import logging
import math

from pathlib import Path
from psycopg2.extensions import AsIs

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import get_current_context
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

logger = logging.getLogger(__name__)


def schemas_tables(config, tenant_id) -> list:
    schemas_tables = []
    config_key = list(config.keys())[0]
    for schema_table in config[config_key]:
        schemas_tables.append(f"{tenant_id}_{schema_table['table_name']}")

    return schemas_tables


def combine_table_counts(**kwargs) -> dict:
    totals = kwargs["record_counts_per_table"]
    tables = kwargs["schema_table"]
    list_totals = list(totals)

    return dict(zip(tables, list_totals))


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


def fetch_record_counts_per_table(**kwargs) -> int:
    context = get_current_context()
    schema_table = kwargs.get("schema", "")

    with open(sql_count_file()) as sqv:
        query = sqv.read()

    result = SQLExecuteQueryOperator(
        task_id="postgres_count_query",
        conn_id="postgres_folio",
        database=kwargs.get("database", "okapi"),
        sql=query,
        parameters={"schema_name": AsIs(schema_table)},
    ).execute(
        context
    )  # type: ignore

    count = result[0][0]
    logger.info(f"Record count: {count}")
    return int(count)


def sql_count_file() -> Path:
    sql_path = Path("/opt/bitnami/airflow") / "plugins/git_plugins/sql/counts.sql"

    return sql_path


def fetch_records_batch_for_table(table, offset, limit, **kwargs) -> list:
    context = get_current_context()

    logger.info(
        f"Selecting records batch for table: {table}, offset: {offset}, limit: {limit}"
    )

    with open(sql_selections_file()) as sqv:
        query = sqv.read()

    result = SQLExecuteQueryOperator(
        task_id="postgres_count_query",
        conn_id="postgres_folio",
        database=kwargs.get("database", "okapi"),
        sql=query,
        parameters={
            "table": AsIs(table),
            "offset": AsIs(offset),
            "limit": AsIs(limit),
        },
    ).execute(
        context
    )  # type: ignore

    return result


def sql_selections_file() -> Path:
    sql_path = Path("/opt/bitnami/airflow") / "plugins/git_plugins/sql/selections.sql"

    return sql_path


def trigger_anonymize_dag(data, config, tenant_id) -> TriggerDagRunOperator:
    """
    Triggers anonymize_data DAG with data, anonymize config, and tenant ID
    {
        'table': 'diku_mod_organizations_storage.contacts',
        'ranges': [(0, 38)]
    }
    """
    trigger_dag = TriggerDagRunOperator(
        task_id="trigger_anonymize_data_dag",
        trigger_dag_id="anonymize_data",
        conf={
            "tenant": tenant_id,
            "table_config": config,
            "data": data,
        },
    )
    logger.info(f"Triggered anonymize_data DAG for {config['table_name']}")

    return trigger_dag


def construct_anon_config(configuration, table) -> dict:
    config: dict = {}
    table_no_tenant = table.split('_', 1)[1]
    conf_key = list(configuration.keys())[0]
    config_tables = configuration[conf_key]
    for schema_table in config_tables:
        if schema_table["table_name"] == table_no_tenant:
            config = schema_table
            config["table_name"] = table

    return config
