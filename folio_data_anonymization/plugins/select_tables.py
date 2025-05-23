import logging

from pathlib import Path

from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

logger = logging.getLogger(__name__)


def schemas_tables(config) -> list:
    schemas_tables = []
    config_key = list(config.keys())[0]
    tenant_id = Variable.get("TENANT_ID", "diku")
    for schema_table in config[config_key]:
        schemas_tables.append(f"{tenant_id}_{schema_table["table_name"]}")

    return schemas_tables


def fetch_number_of_records(**kwargs) -> int:
    context = get_current_context()
    schema_table = kwargs.get("schema", "")

    with open(sql_count_file()) as sqv:
        query = sqv.read()

    # result = SQLExecuteQueryOperator(
    #     task_id="postgres_count_query",
    #     conn_id="postgres_folio",
    #     database=kwargs.get("database", "okapi"),
    #     sql=query,
    #     parameters={"schema_name": schema_table},
    # ).execute(
    #     context
    # )  # type: ignore
    result = ((5123, ()), ())

    count = result[0][0]
    logger.info(f"Record count: {count}")
    return int(count)


def sql_count_file() -> Path:
    sql_path = Path("/opt/bitnami/airflow") / "plugins/git_plugins/sql/counts.sql"

    return sql_path


def do_anonymize(div, start, stop) -> None:
    logger.info(f"DIV: {div}")
    logger.info(f"START: {start}")
    logger.info(f"STOP: {stop}")

    return None
