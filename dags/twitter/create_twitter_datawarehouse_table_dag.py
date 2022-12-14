from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (BigQueryCreateEmptyDatasetOperator, BigQueryExecuteQueryOperator)

from helpers.queries import SqlQueries

from airflow.models import Variable

import datetime 

bigquery_project = Variable.get("bigquery_project_id")
datawarehouse_tweets_dataset = Variable.get("datawarehouse_tweets_dataset") 
raw_twitter_dataset = Variable.get("raw_twitter_dataset") 
raw_tweets_table = Variable.get("raw_tweets_table") 

def replace_sql_variables(query, raw_twitter_dataset, raw_tweets_table): 
    query = (
        query
            .replace("${raw_twitter_dataset}", raw_twitter_dataset)
            .replace("${raw_tweets_table}", raw_tweets_table)
    ) 

    return query

default_args = {
    "owner": "ricardo",
    "start_date": datetime.datetime.now(),
    "depends_on_past": False,
    "retries": 0,
    "catchup": False,
    "email_on_retry": False
}

dag = DAG(
    "create_twitter_datawarehouse",
    default_args=default_args,
    description="WIP",
    max_active_runs=1,
    schedule_interval="@daily"
)

create_datawarehouse_dataset_task = BigQueryCreateEmptyDatasetOperator(
    task_id="create_datawarehouse_dataset",
    dag=dag,
    gcp_conn_id="gcp_boticario_de_case",
    project_id=bigquery_project,
    dataset_id=datawarehouse_tweets_dataset,
    exists_ok=True
)

create_tweets_table_task = BigQueryExecuteQueryOperator(
    task_id="create_tweets_table",
    dag=dag,
    destination_dataset_table=f"{bigquery_project}.{datawarehouse_tweets_dataset}.tweets",
    gcp_conn_id="gcp_boticario_de_case",
    sql=replace_sql_variables(SqlQueries.DATAWAREHOUSE_TWEETS_QUERY, raw_twitter_dataset, raw_tweets_table),
    use_legacy_sql=False,
    write_disposition="WRITE_TRUNCATE",
)

create_datawarehouse_dataset_task >> create_tweets_table_task