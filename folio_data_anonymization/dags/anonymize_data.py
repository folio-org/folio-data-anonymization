"""Anonymize Tables in FOLIO based on Configuration File."""

import logging
from datetime import timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context


logger = logging.getLogger(__name__)

default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    "anonymize_data",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["anonymize"],
) as dag:

    @task
    def setup(**kwargs) -> dict:
        """
        Setup task to prepare the environment for anonymization.
        """
        task_instance = kwargs["ti"]
        params = kwargs.get("params", {})
        # select dag will pass json config file as dict
        config: dict = params.get("configuration", {})
        # select dag will pass tuples of ((id, jsonb), (id, jsonb)...)
        data: tuple = params.get(
            "data",
        )
        tenant = Variable.get("TENANT", "diku")
        logger.info(f"Anonymizing data for {tenant}")
        task_instance.xcom_push(key="tenant", value=tenant)
        task_instance.xcom_push(key="data", value=data)
        return config

    @task(map_index_template="{{ table_name }}")
    def anonymize_table(table_info: dict, **kwargs):
        """
        Anonymize a specific table based on the provided configuration.
        """
        context = get_current_context()
        # context["table_name"] = table_info
        schema_table_name = context.get("table_name")
        task_instance = kwargs["ti"]
        tenant = task_instance.xcom_pull(key="tenant")
        table_name = f"{tenant}_{schema_table_name}"
        data = task_instance.xcom_pull(key="data")
        logger.info(f"Processing {len(data)} records from {table_name}")

    config = setup()
    anonymize_table.expand(table_info=config)
