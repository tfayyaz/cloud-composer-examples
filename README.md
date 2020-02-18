# Cloud Composer Examples

This folder has multiple Cloud Composer DAG examples. Use the settings below to create a cloud composer env first

### Create Cloud Composer Environment

[Create a Cloud Composer environment](https://console.cloud.google.com/composer/environments/create) using the Cloud Console. 

It can take up to 1 hour for the environment to be ready.

Leave all the options as default apart from the following:

- **Name:** composer-prod
- **Location:** Any location
- **Image version:** Latest version
- **Python version:** 3

Under **Airflow configuration** overrides set the following:

- Section: core
- Key: dags_are_paused_at_creation
- Value: True

By default this is set to false which means your DAGs will run as soon as they are uploaded. By changing this to True they will be paused.



