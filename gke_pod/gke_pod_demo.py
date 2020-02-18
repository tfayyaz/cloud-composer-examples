# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""An example DAG demonstrating GKE Create, Delete & Pod Operator"""

import datetime

from airflow import models
from airflow.contrib.kubernetes import pod
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators import kubernetes_pod_operator
from airflow.contrib.operators import gcp_container_operator

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

with models.DAG(
        dag_id='gke_pod_demo',
        schedule_interval=datetime.timedelta(days=1),
        start_date=YESTERDAY) as dag:

    cluster_def = {'name': 'composer-data-processing',
                   'initial_node_count': '2',
                   'node_config': {
                        'oauth_scopes':
                        ['https://www.googleapis.com/auth/compute', 
                        'https://www.googleapis.com/auth/devstorage.full_control',
                        'https://www.googleapis.com/auth/devstorage.read_write']
                   }}

    cluster_create = gcp_container_operator.GKEClusterCreateOperator(
        task_id='cluster_create',
        project_id='cloud-composer-demo',
        location='europe-west1-b',
        body=cluster_def)

    gke_pod = gcp_container_operator.GKEPodOperator(
        # The ID specified for the task.
        task_id='gke_pod',
        project_id='cloud-composer-demo',
        location='europe-west1-b',
        cluster_name='composer-data-processing',
        name='pod-process-data',
        namespace='default',
        image='gcr.io/cloud-builders/gsutil',
        cmds=['gsutil'],
        arguments=['cp',
        'gs://composer-data-team-london/fda_food_events.avro',
        'gs://composer-data-team-amsterdam/fda_food_events_processed_{{ ds }}.avro'],
        )

    cluster_delete = gcp_container_operator.GKEClusterDeleteOperator(
        task_id='cluster_delete',
        project_id='cloud-composer-demo',
        location='europe-west1-b',
        name='composer-data-processing')

    cluster_create >> gke_pod >> cluster_delete
