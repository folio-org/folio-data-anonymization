import json
import logging
import psycopg2

from pathlib import Path

from airflow.sdk import get_current_context
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

logger = logging.getLogger(__name__)


def tables_list(**kwargs):
    with open(tables_json_file(), 'r') as file:
        json_list = json.load(file)

    params = kwargs.get("params", {})
    tenant_id = params.get("tenant_id", "diku")
    tenant_json_list = [f"{tenant_id}_" + item for item in json_list]

    return ','.join(tenant_json_list)


def truncate_db_objects(schemas_tables, **kwargs):
    context = get_current_context()

    with open(sql_file()) as sqv:
        query = sqv.read()

    logger.info(f"Will truncate {schemas_tables}")

    truncation_result = SQLExecuteQueryOperator(
        task_id="postgres_truncate_query",
        conn_id="postgres_folio",
        database=kwargs.get("database", "okapi"),
        sql=query,
        parameters={
            "schemas_tables": psycopg2.extensions.AsIs(schemas_tables),
        },
    ).execute(context)

    return truncation_result


def tables_json_file(**kwargs) -> Path:
    _path = (
        Path(kwargs.get("airflow", "/opt/bitnami/airflow"))
        / "plugins/git_plugins/truncate/truncate_schemas_tables.json"
    )

    return _path


def sql_file(**kwargs) -> Path:
    sql_path = (
        Path(kwargs.get("airflow", "/opt/bitnami/airflow"))
        / "plugins/git_plugins/sql/truncate_schemas_tables.sql"
    )

    return sql_path
