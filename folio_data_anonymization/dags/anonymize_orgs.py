"""Anonymize Tables in FOLIO based on Configuration File."""
import json

from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.decorators import task

from folio_data_anonymization.plugins.utils import (
    retreive_data,
    update_tables
)

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
    params={
        "config": "/opt/airflow/folio_data_anonymization/plugins/config/anonymize_organization_tables.json",
    }
) as dag:

    @task
    def setup(**kwargs):
        """
        Setup task to prepare the environment for anonymization.
        """
        params = kwargs["params"]
        config_path = params["config"]

        with open(config_path) as config_file:
            config = json.load(config_file)
        return config

    @task
    def retrieve_data(table_info: dict) -> list:
        """
        Retrieves table data based on the provided configuration.
        """
        return retreive_data(table_info)


    @task
    def anonymize_table(table_data: list):
        """
        Anonymize data from an organization table.
        """
        return update_tables(table_data)




    config = setup()
    table_data = retrieve_data.expand(table_info=config)
    anonymize_table(table_data)