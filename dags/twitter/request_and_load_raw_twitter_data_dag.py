from airflow import DAG

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

from operators.request_twitter_data import RequestTwitterDataOperator
from hooks.get_top_product import GetTopSoldProductHook

from airflow.models import Variable

import datetime

destination_twitter_bucket = Variable.get("twitter_data_bucket")
bigquery_project = Variable.get("bigquery_project_id")

datawarehouse_sales_dataset = Variable.get("datawarehouse_sales_dataset") 
raw_twitter_dataset = Variable.get("raw_twitter_dataset") 
raw_tweets_table = Variable.get("raw_tweets_table") 

default_args = {
    "owner": "ricardo",
    "start_date": datetime.datetime.now(),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": 60,
    "catchup": False,
    "email_on_retry": False
}

dag = DAG(
    "request_and_load_twitter_data",
    default_args=default_args,
    description="WIP",
    max_active_runs=1,
    schedule_interval="@daily"
)

request_twitter_data_task = RequestTwitterDataOperator(
    task_id="request_twitter_data",
    dag=dag,
    gcp_conn_id="gcp_boticario_de_case",
    product=GetTopSoldProductHook(gcp_conn_id="gcp_boticario_de_case", datawarehouse_sales_dataset=datawarehouse_sales_dataset).return_top_product(month=12, year=2019),
    destination_bucket=destination_twitter_bucket,
    lang="pt",
)
 
create_raw_dataset_task = BigQueryCreateEmptyDatasetOperator(
    task_id="create_raw_dataset",
    dag=dag,
    gcp_conn_id="gcp_boticario_de_case",
    project_id=bigquery_project,
    dataset_id=raw_twitter_dataset,
    exists_ok=True
)

load_raw_table_json_task = GCSToBigQueryOperator(
    task_id="load_raw_table_json",
    dag=dag,
    source_format="NEWLINE_DELIMITED_JSON",
    gcp_conn_id="gcp_boticario_de_case",
    bucket=destination_twitter_bucket,
    source_objects=GCSHook(gcp_conn_id="gcp_boticario_de_case").list(bucket_name=destination_twitter_bucket, delimiter=".json"),
    destination_project_dataset_table=f"{bigquery_project}.{raw_twitter_dataset}.{raw_tweets_table}",
    schema_fields=[
        {
            "name": "id", 
            "type": "STRING", 
            "mode": "NULLABLE"
        },
        {
            "name": "author_id", 
            "type": "STRING", 
            "mode": "NULLABLE"
        },
        {
            "name": "created_at", 
            "type": "TIMESTAMP", 
            "mode": "NULLABLE"
        },
        {
            "name": "public_metrics", 
            "type": "RECORD", 
            "mode": "NULLABLE", 
            "fields": [
                {
                    "name": "retweet_count",
                    "type": "INT64",
                    "mode": "NULLABLE"
                }, 
                {
                    "name": "reply_count",
                    "type": "INT64",
                    "mode": "NULLABLE"
                },
                {
                    "name": "like_count",
                    "type": "INT64",
                    "mode": "NULLABLE"
                },
                {
                    "name": "quote_count",
                    "type": "INT64",
                    "mode": "NULLABLE"
                },
            ]
        },
        {
            "name": "text", 
            "type": "STRING", 
            "mode": "NULLABLE"
        },
        {
            "name": "searched_product", 
            "type": "STRING", 
            "mode": "NULLABLE"
        },
        {
            "name": "ingested_at", 
            "type": "TIMESTAMP", 
            "mode": "NULLABLE"
        },
    ],
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_APPEND",
    time_partitioning = {
        "type_": "DAY",
        "field": "ingested_at"
    },
)

request_twitter_data_task >> create_raw_dataset_task >> load_raw_table_json_task
