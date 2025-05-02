import logging

from pathlib import Path

from airflow.operators.python import get_current_context
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


logger = logging.getLogger(__name__)


def truncate_db_objects(**kwargs):
    context = get_current_context()

    with open(sql_file()) as sqv:
        query = sqv.read()

    schemas_tables = ','.join(kwargs.get('schemas_tables', []))
    logger.info(f"Will truncate {schemas_tables}")

    truncation_result = SQLExecuteQueryOperator(
        task_id="postgres_truncate_query",
        conn_id="postgres_folio",
        database=kwargs.get("database", "okapi"),
        sql=query,
        parameters={
            "schemas_tables": schemas_tables,
        },
    ).execute(context)

    return truncation_result


def sql_file(**kwargs) -> Path:
    sql_path = (
        Path(kwargs.get("airflow", "/opt/airflow"))
        / "../plugins/sql/truncate_schemas_tables.sql"
    )

    return sql_path
