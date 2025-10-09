import json
import logging

from psycopg2.extensions import AsIs
from psycopg2 import Error
from typing import Union

from faker import Faker
from jsonpath_ng import parse

from airflow.exceptions import AirflowFailException

try:
    from plugins.git_plugins.providers import Organizations, Users
except (ImportError, ModuleNotFoundError):
    from folio_data_anonymization.plugins.providers import Organizations, Users

logger = logging.getLogger(__name__)

faker = Faker()
faker.add_provider(Organizations)
faker.add_provider(Users)


def _data_type(data) -> Union[str, list, dict]:
    if isinstance(data, list):
        return []
    elif isinstance(data, dict):
        return {}
    else:
        return ""


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
        data = [match.value for match in expr.find(jsonb)]
        if len(data) > 0:
            replacement_char = _data_type(data.pop())
        else:
            replacement_char = ""
        expr.update(jsonb, replacement_char)
    return jsonb


def update_row(**kwargs) -> Union[bool, None]:
    row_uuid: str = kwargs['id']
    jsonb: dict = kwargs['jsonb']
    schema_table: str = kwargs['schema_table']
    connection = kwargs.get("connection")
    json_obj = json.dumps(jsonb)

    logger.info(f"Updating: {json_obj}")
    try:
        cursor = connection.cursor()  # type: ignore
        sql = "UPDATE %(table)s SET jsonb=%(jsonb)s WHERE id=%(id)s"
        params = {
            "table": AsIs(schema_table),
            "jsonb": json_obj,
            "id": row_uuid,
        }
        cursor.execute(sql, params)
        connection.commit()  # type: ignore
        logger.info(f"Successfully updated {schema_table} uuid {row_uuid}")
        return True
    except Error as e:
        raise AirflowFailException(
            f"Failed updating {schema_table} uuid {row_uuid} - {e}"
        )
