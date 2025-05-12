"""Anonymize Tables in FOLIO based on Configuration File."""
import json

from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.decorators import task


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
        "tenant": "sul"
    }
) as dag:

    @task
    def setup(**kwargs):
        """
        Setup task to prepare the environment for anonymization.
        """
        task_instance = kwargs["ti"]
        params = kwargs["params"]
        config_path = params["config"]
        tenant = params["tenant"]
        task_instance.xcom_push(key="tenant", value=tenant)
        with open(config_path) as config_file:
            config = json.load(config_file)
        return config

    @task
    def anonymize_table(table_info: dict, **kwargs):
        """
        Anonymize a specific table based on the provided configuration.
        """
        task_instance = kwargs["ti"]
        tenant = task_instance.xcom_pull(key="tenant")


    config = setup()
    anonymize_table.expand(table_info=config)




        
