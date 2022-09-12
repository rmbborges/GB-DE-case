import logging
from io import StringIO

from unidecode import unidecode

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.google.cloud.hooks.gcs import GCSHook

import pandas as pd

class TreatXLSXOperator(BaseOperator):

    @apply_defaults
    def __init__(
        self,
        gcp_conn_id="",
        source_bucket="",
        destination_bucket="",
        desination_file_type="csv",
        *args, 
        **kwargs
    ):

        super(TreatXLSXOperator, self).__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket
        self.desination_file_type = desination_file_type

    def execute(self, context):
        gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
        list_of_files = gcs_hook.list(bucket_name=self.source_bucket)

        for file_name in list_of_files:
            file_data = gcs_hook.download(bucket_name=self.source_bucket, object_name=file_name)   
            
            dataframe = pd.read_excel(file_data, engine="openpyxl")
            dataframe = dataframe.assign(MARCA=lambda x: x["MARCA"].apply(unidecode))
            
            csv_buffer = StringIO()
            dataframe.to_csv(csv_buffer, index=False)

            gcs_hook.upload(bucket_name=self.destination_bucket, object_name=file_name.replace(".xlsx", ".csv"), data=csv_buffer.getvalue(), encoding="utf-8")
 