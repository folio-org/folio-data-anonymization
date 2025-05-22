import logging

from pathlib import Path

from airflow.operators.python import get_current_context
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

logger = logging.getLogger(__name__)


def fetch_number_of_records(**kwargs) -> int:
    context = get_current_context()
    schema_table_name = kwargs.get("schema", "")

    with open(sql_count_file()) as sqv:
        query = sqv.read()

    result = SQLExecuteQueryOperator(
        task_id="postgres_count_query",
        conn_id="postgres_folio",
        database=kwargs.get("database", "okapi"),
        sql=query,
        parameters={"schema_name": schema_table_name},
    ).execute(
        context
    )  # type: ignore

    count = result[0][0]
    logger.info(f"Record count: {count}")
    return int(count)


def sql_count_file():
    sql_path = Path("/opt/bitnami/airflow") / "plugins/git_plugins/sql/counts.sql"

    return sql_path
