"""Anonymize Tables in FOLIO based on Configuration File."""

import logging
from datetime import timedelta

from airflow.sdk import dag
from airflow.providers.standard.operators.empty import EmptyOperator


try:
    from plugins.git_plugins.anonymize import (
        anonymize_payload,
        anonymize_row_update_table,
        payload_tuples,
    )
except (ImportError, ModuleNotFoundError):
    from folio_data_anonymization.plugins.anonymize import (
        anonymize_payload,
        anonymize_row_update_table,
        payload_tuples,
    )

try:
    from plugins.git_plugins.sql_pool import SQLPool
except (ImportError, ModuleNotFoundError):
    from folio_data_anonymization.plugins.sql_pool import SQLPool


logger = logging.getLogger(__name__)

default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    "anonymize_data",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["anonymize"],
    render_template_as_native_obj=True,
    max_active_runs=1,
    max_active_tasks=10,
)
def anonymize():
    connection_pool = SQLPool().pool()

    payload = anonymize_payload()

    all_rows = payload_tuples(payload=payload)

    anonymize_row_update_table.partial(
        payload=payload, connection_pool=connection_pool
    ).expand(data=all_rows) >> EmptyOperator(task_id="Finished")


anonymize()
