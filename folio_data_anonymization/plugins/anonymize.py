import logging

from airflow.sdk import task

try:
    from plugins.git_plugins.utils import fake_jsonb, update_row
except (ImportError, ModuleNotFoundError):
    from folio_data_anonymization.plugins.utils import fake_jsonb, update_row

logger = logging.getLogger(__name__)


@task
def anonymize_payload(**kwargs):
    """
    Setup task to prepare the environment for anonymization.
    select_tables DAG will pass:
    conf = {"tenant": "",
            "table_config":
                {"table_name": "diku_mod_users.users",
                    "anonymize": {"jsonb": []}},
                    "set_to_empty": {"jsonb": []}},
            "data": (('id1', 'jsonb1'), ('id2', 'jsonb2'),...n)
            }
    """
    params = kwargs.get("params", {})
    table_config: dict = params.get("table_config", {})
    data: list = params.get("data", [])
    tenant = params.get("tenant", "diku")
    logger.info(f"Anonymizing data for tenant {tenant}")
    logger.info(
        f"Begin processing {len(data)} records from {table_config.get('table_name')}"  # noqa
    )
    return {"config": table_config, "data": data}


@task
def payload_tuples(**kwargs):
    return kwargs["payload"]["data"]


@task
def anonymize_row_update_table(**kwargs):
    data: tuple = kwargs["data"]
    config: dict = kwargs["payload"]["config"]
    connection_pool = kwargs.get("connection_pool")
    logger.info(f"Anonymizing record {data[0]}")

    fake_json = fake_jsonb(data[1], config)
    logger.info(f"Processed data: {fake_json}")
    conn = connection_pool.getconn()
    update_row(
        id=data[0],
        jsonb=fake_json,
        schema_table=config["table_name"],
        connection=conn,
    )
    connection_pool.putconn(conn)
