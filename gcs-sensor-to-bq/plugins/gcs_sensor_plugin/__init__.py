from airflow.plugins_manager import AirflowPlugin
from gcs_sensor_plugin.sensors.gcs_sensor_plugin import GoogleCloudStorageUploadSessionCompleteSensor

class GoogleCloudStorageUploadSessionCompleteSensor(AirflowPlugin):
    name = "gcs_sensor_plugin"
    sensors = [GoogleCloudStorageUploadSessionCompleteSensor]