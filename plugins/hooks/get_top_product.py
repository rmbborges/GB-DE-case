import logging

from airflow.hooks.base import BaseHook
from airflow.utils.decorators import apply_defaults
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from helpers.queries import SqlQueries

class GetTopSoldProductHook(BaseHook):

    @apply_defaults
    def __init__(
        self,
        gcp_conn_id="",
        datawarehouse_sales_dataset="",
        *args, 
        **kwargs
    ):

        super(GetTopSoldProductHook, self).__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.datawarehouse_sales_dataset = datawarehouse_sales_dataset

    @staticmethod
    def _fill_query(datawarehouse_sales_dataset, month, year):
        query = (
            SqlQueries
                .TOP_PRODUCT_QUERY
                .replace("${datawarehouse_sales_dataset}", datawarehouse_sales_dataset)
                .replace("${month}", str(month))
                .replace("${year}", str(year))
        )
        
        return query 

    def return_top_product(self, month, year):
        query = self._fill_query(datawarehouse_sales_dataset=self.datawarehouse_sales_dataset, month=month, year=year)
        bigquery_hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id, use_legacy_sql=False)
        conn = bigquery_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()

        if len(results[0]) > 0: 
            return results[0][0]
        else:
            raise Exception("Unable to retrieve the top sold product for the requested period.")
