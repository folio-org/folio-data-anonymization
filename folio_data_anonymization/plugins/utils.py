import json
import logging
import pathlib
import uuid

import psycopg2

from faker import Faker
from jsonpath_ng import parse

from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from folio_data_anonymization.plugins.providers import Organizations

logger = logging.getLogger(__name__)

faker = Faker()
faker.add_provider(Organizations)


def fake_jsonb(jsonb: dict, config: dict) -> dict:
    """
    Fake the jsonb data based on the provided config.
    """
    for row in config["anonymize"]["jsonb"]:
        expr = parse(row[0])
        faker_function = getattr(faker, row[1])
        expr.update(jsonb, faker_function())
    for row in config.get("set_to_empty", {}).get("jsonb", []):
        expr = parse(row)
        expr.update(jsonb, "")
    return jsonb


def retreive_data(**kwargs) -> dict:
    """
    Retrieves table data based on configuration
    """
    context = get_current_context()
    tenant_id = Variable.get("TENANT_ID", "diku")
    airflow = kwargs.get("airflow", "/opt/bitnami/airflow/")
    select_sql = _get_sql_file("retrieve_data.sql", airflow)
    table_config = kwargs.get("table_config")
    schema_table = f"{tenant_id}_{table_config['table_name']}"
    database = kwargs.get("database", "okapi")
    table_results = SQLExecuteQueryOperator(
        task_id="retrieve_org_data",
        conn_id="postgres_folio",
        database=database,
        sql=select_sql,
        parameters={
            "schema_table": psycopg2.extensions.AsIs(schema_table),
        },
    ).execute(context)
    temp_file_path = pathlib.Path(airflow) / f"tmp/{uuid.uuid4()}/{schema_table}"
    temp_file_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"Writing {len(table_results):,} rows to {temp_file_path}")
    with temp_file_path.open("w+") as fo:
        for row in table_results:
            fo.write(f"{json.dumps(row)}\n")
    return {
        "file_path": str(temp_file_path),
        "database": database,
        "config": table_config
    }


def update_table(table_data: dict):
    """
    Updates table with anonymized data
    """
    results_path = pathlib.Path(table_data['file_path'])
    update_sql = _get_sql_file()
    with results_path.open() as fo:
        for row in fo.readlines():
            payload = json.loads(row)
            jsonb = fake_jsonb(payload['jsonb'])
            _update_row(jsonb, table_data['database'], update_sql)


def _get_sql_file(file_name: str, airflow: str) -> str:
    sql_path = (
        pathlib.Path(airflow)
        / f"plugins/git_folio-data-anonymization/sql/{file_name}"
    )
    return sql_path


def _update_row(**kwargs) -> bool:
    context = kwargs["context"]
    row_uuid: str = kwargs['uuid']
    jsonb: dict = kwargs['jsonb']
    database_name: str = kwargs['database']
    update_sql: str = kwargs['sql']
    schema_table: str = kwargs['schema_name']
    try:
        SQLExecuteQueryOperator(
            task_id="update_org_data",
            conn_id="postgres_folio",
            database=database_name,
            sql=update_sql,
            parameters={
                "schema_table": psycopg2.extensions.AsIs(schema_table),
                "jsonb": psycopg2.extensions.AsIs(jsonb),
                "id": psycopg2.extensions.AsIs(row_uuid)
            },
        ).execute(context)
        return True
    except Exception as e:
        logger.error(f"Failed updating {schema_table} uuid {row_uuid} - {e}")
        return False


