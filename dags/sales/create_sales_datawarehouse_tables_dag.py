from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (BigQueryCreateEmptyDatasetOperator, BigQueryExecuteQueryOperator)

from helpers.queries import SqlQueries

from airflow.models import Variable

import datetime 

bigquery_project = Variable.get("bigquery_project_id")
datawarehouse_sales_dataset = Variable.get("datawarehouse_sales_dataset") 
raw_sales_dataset = Variable.get("raw_sales_dataset") 
raw_sales_table = Variable.get("raw_sales_table") 

def replace_sql_variables(query, raw_sales_dataset, raw_sales_table): 
    query = (
        query
            .replace("${raw_sales_dataset}", raw_sales_dataset)
            .replace("${raw_sales_table}", raw_sales_table)
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
    "create_sales_datawarehouse",
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
    dataset_id=datawarehouse_sales_dataset,
    exists_ok=True
)

create_general_monthly_sales_table_task = BigQueryExecuteQueryOperator(
    task_id="create_general_monthly_sales_table",
    dag=dag,
    destination_dataset_table=f"{bigquery_project}.{datawarehouse_sales_dataset}.general_monthly_sales",
    gcp_conn_id="gcp_boticario_de_case",
    sql=replace_sql_variables(SqlQueries.GENERAL_MONTHLY_SALES_TABLE_QUERY, raw_sales_dataset, raw_sales_table),
    use_legacy_sql=False,
    write_disposition="WRITE_TRUNCATE",
)

create_product_general_sales_table_task = BigQueryExecuteQueryOperator(
    task_id="create_product_general_sales_table",
    dag=dag,
    destination_dataset_table=f"{bigquery_project}.{datawarehouse_sales_dataset}.product_general_sales",
    gcp_conn_id="gcp_boticario_de_case",
    sql=replace_sql_variables(SqlQueries.PRODUCT_GENERAL_SALES_TABLE_QUERY, raw_sales_dataset, raw_sales_table),
    use_legacy_sql=False,
    write_disposition="WRITE_TRUNCATE",
)

create_brand_monthly_sales_table_task = BigQueryExecuteQueryOperator(
    task_id="create_brand_monthly_sales_table",
    dag=dag,
    destination_dataset_table=f"{bigquery_project}.{datawarehouse_sales_dataset}.brand_monthly_sales",
    gcp_conn_id="gcp_boticario_de_case",
    sql=replace_sql_variables(SqlQueries.BRAND_MONTHLY_SALES_TABLE_QUERY, raw_sales_dataset, raw_sales_table),
    use_legacy_sql=False,
    write_disposition="WRITE_TRUNCATE",
)

create_product_monthly_sales_table_task = BigQueryExecuteQueryOperator(
    task_id="create_product_monthly_sales_table",
    dag=dag,
    destination_dataset_table=f"{bigquery_project}.{datawarehouse_sales_dataset}.product_monthly_sales",
    gcp_conn_id="gcp_boticario_de_case",
    sql=replace_sql_variables(SqlQueries.PRODUCT_MONTHLY_SALES_TABLE_QUERY, raw_sales_dataset, raw_sales_table),
    use_legacy_sql=False,
    write_disposition="WRITE_TRUNCATE",
)

create_datawarehouse_dataset_task >> [
    create_general_monthly_sales_table_task,
    create_product_general_sales_table_task,
    create_brand_monthly_sales_table_task,
    create_product_monthly_sales_table_task
]
 