from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from airflow.models import Variable

import logging
import requests
import json
from datetime import datetime

bearer_token = Variable.get("twitter_api_bearer_token")

class RequestTwitterDataOperator(BaseOperator):

    @apply_defaults
    def __init__(
        self,
        gcp_conn_id="",
        product="",
        destination_bucket="",
        lang="pt",
        *args, 
        **kwargs
    ):

        super(RequestTwitterDataOperator, self).__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.product = product
        self.lang = lang
        self.destination_bucket = destination_bucket

    @staticmethod
    def generate_headers(bearer_token):
        headers = {
            "Authorization": f"Bearer {bearer_token}",
            "User-Agent": "v1RecentSearchPythonGBCase"
        }

        return headers
        
    def execute(self, context):
        request_endpoint = "https://api.twitter.com/2/tweets/search/all"

        search_term = f"Boticário {self.product} lang:{self.lang}"

        query_params = {
            "query": search_term, 
            "tweet.fields": "id,text,author_id,created_at,public_metrics",
            "sort_order": "recency",
            "max_results": "50"
        }

        response = requests.get(request_endpoint, headers=self.generate_headers(bearer_token), params=query_params)

        logging.info(response.status_code)

        if response.status_code == 200:
            gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
            execution_timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
            object_name = f"request_{execution_timestamp}.json"
            
            data = response.json()["data"]
            
            dump_str = ""
            for tweet in data:
                tweet["searched_product"] = self.product
                tweet["ingested_at"] = execution_timestamp
                dump_str += json.dumps(tweet) + "\n"

            gcs_hook.upload(bucket_name=self.destination_bucket, object_name=object_name, data=dump_str, encoding="utf-8")
        else:
            raise Exception(response.status_code, response.text)