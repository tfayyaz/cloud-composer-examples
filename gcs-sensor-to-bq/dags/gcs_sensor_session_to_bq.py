# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import os

from airflow import models
from gcs_sensor_plugin.sensors.gcs_sensor_plugin import GoogleCloudStorageUploadSessionCompleteSensor
from airflow.contrib.operators import gcs_to_bq
from airflow.utils import trigger_rule

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': yesterday,
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting at least 5 minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': 'cloud-data-ml'
}

# schedule_interval = datetime.timedelta(hour=1)
# schedule_interval = "* 15 * * * *"
# Continue to run DAG once per day
schedule_interval = '@daily'

schema_fields = [
  {
    "description": "user_id",
    "mode": "NULLABLE",
    "name": "user_id",
    "type": "STRING"
  },
  {
    "description": "country",
    "mode": "NULLABLE",
    "name": "country",
    "type": "STRING"
  }
]

# [START composer_quickstart_schedule]
with models.DAG(
    'gcs_sensor_session_to_bq',
    # Continue to run DAG once per day
    schedule_interval=schedule_interval,
    default_args=default_dag_args) as dag:
    # [END composer_quickstart_schedule]

        file_watcher = GoogleCloudStorageUploadSessionCompleteSensor(
            task_id='file_watcher',
            bucket='my-bucket-name',
            prefix='gcs_sensor_to_bq/users_{{ ds_nodash }}',
            inactivity_period=60 * 10,
            poke_interval=60,
            min_objects=5,
            allow_delete=False,
            previous_num_objects=0,
            google_cloud_conn_id='google_cloud_default'
        )

        gcs_import_to_bq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
            task_id='gcs_import_to_bq',
            bucket='cmy-bucket-name',
            source_format='NEWLINE_DELIMITED_JSON',
            source_objects=['gcs_sensor_to_bq/users_{{ ds_nodash }}*'],
            # Set autodetect to True if table created already in BigQuery
            schema_fields=schema_fields,
            autodetect=False,
            destination_project_dataset_table='project-id.gcs_sensor_example.users',
            create_disposition='CREATE_NEVER',
            write_disposition='WRITE_APPEND'
        )

        file_watcher >> gcs_import_to_bq