from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

from airflow.models import Variable

from operators.treat_xlsx import (TreatXLSXOperator)

import datetime

source_sales_data_bucket = Variable.get("raw_sales_data_bucket")
destination_sales_data_bucket = Variable.get("sales_data_bucket") 

bigquery_project = Variable.get("bigquery_project_id")
raw_sales_dataset = Variable.get("raw_sales_dataset") 
raw_sales_table = Variable.get("raw_sales_table") 

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
    "load_raw_sales_data",
    default_args=default_args,
    description="WIP",
    max_active_runs=1,
    schedule_interval="@daily"
)

treat_raw_data_task = TreatXLSXOperator(
    task_id="treat_xlsx_data",
    dag=dag,
    gcp_conn_id="gcp_boticario_de_case",
    source_bucket =source_sales_data_bucket,
    destination_bucket=destination_sales_data_bucket,
    desination_file_type="csv"
)

create_raw_dataset_task = BigQueryCreateEmptyDatasetOperator(
    task_id="create_raw_dataset",
    dag=dag,
    gcp_conn_id="gcp_boticario_de_case",
    project_id=bigquery_project,
    dataset_id=raw_sales_dataset,
    exists_ok=True
)

load_raw_table_csv_task = GCSToBigQueryOperator(
    task_id="load_raw_table_csv",
    dag=dag,
    gcp_conn_id="gcp_boticario_de_case",
    bucket=destination_sales_data_bucket,
    source_objects=GCSHook(gcp_conn_id="gcp_boticario_de_case").list(bucket_name=destination_sales_data_bucket, delimiter=".csv"),
    destination_project_dataset_table=f"{bigquery_project}.{raw_sales_dataset}.{raw_sales_table}",
    schema_fields=[
        {
            "name": "ID_MARCA", 
            "type": "INT64", 
            "mode": "NULLABLE"
        },
        {
            "name": "MARCA", 
            "type": "STRING", 
            "mode": "NULLABLE"
        },
        {
            "name": "ID_LINHA", 
            "type": "INT64", 
            "mode": "NULLABLE"
        },
        {
            "name": "LINHA", 
            "type": "STRING", 
            "mode": "NULLABLE"
        },
        {
            "name": "DATA_VENDA", 
            "type": "DATE", 
            "mode": "NULLABLE"
        },
        {
            "name": "QTD_VENDA", 
            "type": "INT64", 
            "mode": "NULLABLE"
        },
    ],
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE",
    skip_leading_rows=1
)

treat_raw_data_task >> create_raw_dataset_task >> load_raw_table_csv_task
 