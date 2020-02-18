# Test import using CMD line

## Set the gcloud project

export PROJECT=<REPLACE-THIS-WITH-YOUR-PROJECT-ID>
gcloud config set project $PROJECT

gcloud config set project cloud-data-ml

## Create BigQuery dataset

bq mk gcs_sensor_example

## Create BigQuery Table

bq mk -t gcs_sensor_example.users user_id:string,country:string

bq query --use_legacy_sql=false '
CREATE TABLE mydataset.newtable
(
  x INT64 OPTIONS(description="An optional INTEGER field"),
  y STRUCT<
    a ARRAY<STRING> OPTIONS(description="A repeated STRING field"),
    b BOOL
  >
)
PARTITION BY _PARTITIONDATE
OPTIONS(
  expiration_timestamp=TIMESTAMP "2020-01-01 00:00:00 UTC",
  partition_expiration_days=1,
  description="a table that expires in 2020, with each partition living for 24 hours",
  labels=[("org_unit", "development")]
)'

## Upload file to GCS

gsutil cp -L log users_* gs://cloud-data-ml-composer/gcs_sensor_to_bq/

## Check files were uploaded

gsutil ls gs://cloud-data-ml-composer/gcs_sensor_to_bq/

## Import file to BigQuery

bq load --source_format=NEWLINE_DELIMITED_JSON \
gcs_sensor_example.users gs://cloud-data-ml-composer/gcs_sensor_to_bq/users_*

## Check data is in the table

bq query --use_legacy_sql=false '
SELECT * FROM `gcs_sensor_example.users`'

## Delete BigQuery table

bq rm gcs_sensor_example.users

# Create BQ table and upload DAG

## Create BigQuery Table

bq mk -t gcs_sensor_example.users user_id:string,country:string

## Create gcs sensor to BQ DAG

Create dag called gcs_sensor_to_bq.py

## Create composer env

## Get compser env bucket 

gcloud composer environments list --locations europe-west1

gcloud composer environments describe composer-4-nodes-2-cores --location europe-west1

## Upload Plugin to GCS bucket

gsutil cp -r plugins/gcs_sensor_plugin/* gs://europe-west1-composer-4-nod-b2412042-bucket/plugins/gcs_sensor_plugin/

## Upload DAG to GCS bucket

gs://europe-west1-composer-4-nod-b2412042-bucket/dags

gsutil cp dags/gcs_sensor_to_bq.py gs://europe-west1-composer-4-nod-b2412042-bucket/dags
gsutil cp dags/gcs_sensor_session_to_bq.py gs://europe-west1-composer-4-nod-b2412042-bucket/dags
gsutil cp dags/gcs_sensor_session_to_bq_v1.0.py gs://europe-west1-composer-4-nod-b2412042-bucket/dags
gsutil cp dags/gcs_sensor_session_to_bq_v1.3.py gs://europe-west1-composer-4-nod-b2412042-bucket/dags
gsutil cp dags/gcs_sensor_session_to_bq_v1.4.py gs://europe-west1-composer-4-nod-b2412042-bucket/dags
gsutil cp dags/gcs_sensor_session_to_bq_v1.5.py gs://europe-west1-composer-4-nod-b2412042-bucket/dags
gsutil cp dags/gcs_sensor_session_to_bq_v1.6.py gs://europe-west1-composer-4-nod-b2412042-bucket/dags

gsutil cp dags/task_sensor_bq_job_v1.0.py gs://europe-west1-composer-4-nod-b2412042-bucket/dags

gsutil ls gs://europe-west1-composer-4-nod-b2412042-bucket/plugins

## Duplicate file for different dates

for i in {1..5}; do cp data/users_0001.json "data/users_20191106_000$i.json"; done