import logging
import requests
import datetime

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.google.cloud.hooks.gcs import GCSHook

class RequestTweetData(BaseOperator):

    @apply_defaults
    def __init__(
        self,
        gcp_conn_id="",
        start_date=datetime.datetime(2022, 9, 1, 10, 0, 0),
        mentions="",
        language="pt",
        *args, 
        **kwargs
    ):

        super(RequestTweetData, self).__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.start_date = start_date
        self.mentions = mentions
        self.language = language

    def execute(self, context):
        gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)

        # query_params = {'query': search_term, 'space.fields': 'title,created_at', 'expansions': 'creator_id'}
        # request
        return records
 