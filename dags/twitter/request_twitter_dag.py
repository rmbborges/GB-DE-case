from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from hooks.get_top_product import GetTopSoldProductHook

import logging
import datetime

def log_hook(month, year, gcp_conn_id):
    data = GetTopSoldProductHook(month=month, year=year, gcp_conn_id=gcp_conn_id).return_top_product()

    logging.info(data)
    return data

default_args = {
    "owner": "ricardo",
    "start_date": datetime.datetime.now(),
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": 60,
    "catchup": False,
    "email_on_retry": False
}

dag = DAG(
    "request_twitter_data",
    default_args=default_args,
    description="WIP",
    max_active_runs=1,
    schedule_interval="@daily"
)

get_top_product_task = PythonOperator(
    task_id="get_top_product",
    python_callable=log_hook,
    dag=dag,
    op_kwargs={
        "year": 2019,
        "month": 12,
        "gcp_conn_id": "gcp_boticario_de_case"
    }
)

get_top_product_task