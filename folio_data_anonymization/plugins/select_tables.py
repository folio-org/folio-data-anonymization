import logging

from pathlib import Path
from psycopg2.extensions import AsIs

from airflow.models import DagBag
from airflow.sdk import get_current_context, timezone
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.state import State

logger = logging.getLogger(__name__)


def schemas_tables(config, tenant_id) -> list:
    schemas_tables = []
    config_key = list(config.keys())[0]
    for schema_table in config[config_key]:
        schemas_tables.append(f"{tenant_id}_{schema_table['table_name']}")

    return schemas_tables


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


def do_anonymize(tables_and_ranges, configuration, tenant_id) -> None:
    """
    [
        {
            'table': 'diku_mod_organizations_storage.contacts',
            'ranges': [(0, 38)]
        },
        {
            'table': 'diku_mod_organizations_storage.interface_credentials',
            'ranges': [(0, 74)]
        },
        {
            'table': 'diku_mod_organizations_storage.interfaces',
            'ranges': [(0, 76)]
        },
        {
            'table': 'diku_mod_organizations_storage.organizations',
            'ranges': [(0, 100), (100, 200), .. (2400, 2500), (2500, 2600)]
        }
    ]
    """

    for table_ranges in tables_and_ranges:
        table = table_ranges["table"]
        logger.info(f"TABLE: {table}")
        config = constuct_anon_config(configuration, table)

        logger.info(f"CONFIG: {config}")
        for range in table_ranges["ranges"]:
            offset = range[0]
            limit = range[1] - offset

            logger.info(
                f"Selecting records batch for table: {table}, \
                    offset: {offset}, limit {limit}"
            )
            data = fetch_records_batch_for_table(table, offset, limit)
            execution_date = timezone.utcnow()

            dagbag = DagBag("/opt/bitnami/airflow/dags/git_dags")
            dag = dagbag.get_dag("anonymize_data")
            dag_run_id = f"manual__{execution_date.isoformat()}"

            dag.create_dagrun(
                run_id=dag_run_id,
                execution_date=execution_date,
                state=State.QUEUED,
                conf={
                    "tenant": tenant_id,
                    "table_config": config,
                    "data": data,
                },
                external_trigger=True,
            )
            logger.info(f"Anonymizing {table} with OFFSET: {offset} LIMIT: {limit};")

    return None


def constuct_anon_config(configuration, table) -> dict:
    config: dict = {}
    table_no_tenant = table.split('_', 1)[1]
    conf_key = list(configuration.keys())[0]
    config_tables = configuration[conf_key]
    for schema_table in config_tables:
        if schema_table["table_name"] == table_no_tenant:
            config = schema_table
            config["table_name"] = table

    return config
