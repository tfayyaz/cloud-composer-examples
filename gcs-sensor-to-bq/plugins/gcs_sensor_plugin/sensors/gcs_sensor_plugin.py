import os
from datetime import datetime

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

def ts_function(context):
    """
    Default callback for the GoogleCloudStorageObjectUpdatedSensor. The default
    behaviour is check for the object being updated after execution_date +
    schedule_interval.
    """
    return context['dag'].following_schedule(context['execution_date'])

def get_time():
    """
    This is just a wrapper of datetime.datetime.now to simplify mocking in the
    unittests.
    """
    return datetime.now()

class GoogleCloudStorageUploadSessionCompleteSensor(BaseSensorOperator):
    """
    Checks for changes in the number of objects at prefix in Google Cloud Storage
    bucket and returns True if the inactivity period has passed with no
    increase in the number of objects. Note, it is recommended to use reschedule
    mode if you expect this sensor to run for hours.
    :param bucket: The Google cloud storage bucket where the objects are.
        expected.
    :type bucket: str
    :param prefix: The name of the prefix to check in the Google cloud
        storage bucket.
    :param inactivity_period: The total seconds of inactivity to designate
        an upload session is over. Note, this mechanism is not real time and
        this operator may not return until a poke_interval after this period
        has passed with no additional objects sensed.
    :type inactivity_period: int
    :param min_objects: The minimum number of objects needed for upload session
        to be considered valid.
    :type min_objects: int
    :param previous_num_objects: The number of objects found during the last poke.
    :type previous_num_objects: int
    :param inactivity_seconds: The current seconds of the inactivity period.
    :type inactivity_seconds: int
    :param allow_delete: Should this sensor consider objects being deleted
        between pokes valid behavior. If true a warning message will be logged
        when this happens. If false an error will be raised.
    :type allow_delete: bool
    :param google_cloud_conn_id: The connection ID to use when connecting
        to Google cloud storage.
    :type google_cloud_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work,
        the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    """

    template_fields = ('bucket', 'prefix')
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 bucket,
                 prefix,
                 inactivity_period=60 * 60,
                 min_objects=1,
                 previous_num_objects=0,
                 allow_delete=True,
                 google_cloud_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args, **kwargs):

        super(GoogleCloudStorageUploadSessionCompleteSensor, self).__init__(*args, **kwargs)

        self.bucket = bucket
        self.prefix = prefix
        self.inactivity_period = inactivity_period
        self.min_objects = min_objects
        self.previous_num_objects = previous_num_objects
        self.inactivity_seconds = 0
        self.allow_delete = allow_delete
        self.google_cloud_conn_id = google_cloud_conn_id
        self.delegate_to = delegate_to
        self.last_activity_time = None

    def is_bucket_updated(self, current_num_objects):
        """
        Checks whether new objects have been uploaded and the inactivity_period
        has passed and updates the state of the sensor accordingly.
        :param current_num_objects: number of objects in bucket during last poke.
        :type current_num_objects: int
        """

        if current_num_objects > self.previous_num_objects:
            # When new objects arrived, reset the inactivity_seconds
            # previous_num_objects for the next poke.
            self.log.info(
                '''
                New objects found at {} resetting last_activity_time.
                '''.format(os.path.join(self.bucket, self.prefix)))
            self.last_activity_time = get_time()
            self.inactivity_seconds = 0
            self.previous_num_objects = current_num_objects
        elif current_num_objects < self.previous_num_objects:
            # During the last poke interval objects were deleted.
            if self.allow_delete:
                self.previous_num_objects = current_num_objects
                self.last_activity_time = get_time()
                self.log.warning(
                    '''
                    Objects were deleted during the last
                    poke interval. Updating the file counter and
                    resetting last_activity_time.
                    '''
                )
            else:
                raise RuntimeError(
                    '''
                    Illegal behavior: objects were deleted in {} between pokes.
                    '''.format(os.path.join(self.bucket, self.prefix))
                )
        else:
            if self.last_activity_time:
                self.inactivity_seconds = (
                    get_time() - self.last_activity_time).total_seconds()
            else:
                # Handles the first poke where last inactivity time is None.
                self.last_activity_time = get_time()
                self.inactivity_seconds = 0

            if self.inactivity_seconds >= self.inactivity_period:
                if current_num_objects >= self.min_objects:
                    self.log.info(
                        '''
                        SUCCESS:
                        Sensor found {} objects at {}.
                        Waited at least {} seconds, with no new objects dropped.
                        '''.format(
                            current_num_objects,
                            os.path.join(self.bucket, self.prefix),
                            self.inactivity_period))
                    return True

                warn_msg = \
                    '''
                    FAILURE:
                    Inactivity Period passed,
                    not enough objects found in {}
                    '''.format(
                        os.path.join(self.bucket, self.prefix))
                self.log.warning(warn_msg)
                return False
            return False

    def poke(self, context):
        hook = GoogleCloudStorageHook()
        return self.is_bucket_updated(len(hook.list(self.bucket, prefix=self.prefix)))