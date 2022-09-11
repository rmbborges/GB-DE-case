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
        month="",
        year="",
        *args, 
        **kwargs
    ):

        super(GetTopSoldProductHook, self).__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.month=month
        self.year=year

    @staticmethod
    def _fill_query(month, year):
        query = (
            SqlQueries
                .TOP_PRODUCT_QUERY
                .replace("${month}", str(month))
                .replace("${year}", str(year))
        )
        
        return query 

    def return_top_product(self):
        query = self._fill_query(self.month, self.year)
        bigquery_hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id, use_legacy_sql=False)
        conn = bigquery_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()

        return results[0][0]
