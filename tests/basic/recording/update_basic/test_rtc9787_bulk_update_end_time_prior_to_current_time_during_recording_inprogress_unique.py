import Queue
import logging
import multiprocessing.pool as mp_pool
import os
import pytest
import requests
import datetime

import helpers.constants as constants
import helpers.models.recording as recording_model
import helpers.notification.utils as notification_utils
import helpers.utils as utils
import helpers.vmr.a8.a8_helper as a8
import helpers.vmr.rio.api as rio
import validators.validate_common as validate_common
import validators.validate_recordings as validate_recordings

from helpers.constants import RecordingAttribute
from helpers.constants import TestLog
from helpers.constants import TimeFormat

pytestmark = pytest.mark.recording
LOGGER = logging.getLogger(os.environ.get("PYTEST_XDIST_WORKER", TestLog.TEST_LOGGER))

TOTAL_RECORDINGS_LIST = [20]

TC9787_DATA = [
    ("test_rtc9787_bulk_update_end_time_prior_to_current_time_during_recording_inprogress_unique",
    RecordingAttribute.COPY_TYPE_UNIQUE)]


@utils.test_case_logger
@pytest.mark.parametrize("total_recording", TOTAL_RECORDINGS_LIST)
@pytest.mark.parametrize("name, copy_type", TC9787_DATA, ids=[x[0] for x in TC9787_DATA])
def test_rtc9787_bulk_update_end_time_prior_to_current_time_during_recording_inprogress_unique(
        total_recording, stream, name, copy_type):
    """
    TC9787: Bulk update of recording requests of end time prior to current time during recording
    in progress (20 requests) unique copy
    1. Send a bulk request for 20 unique copy recordings.
    2. Change the end time for all the recordings during the recording is in-progress.
    Verify that bulk update of end time during recording gets failed.

    UPDATED 06/14/2019 to look for success in changing end time - From VMR development team:
    "RM on getting /a8/record request - updates the end time if the recording has not already ended
    and if end time in the request is in valid ISO8601 format. It returns success response."
    """
    recording = None
    web_service_objects = []
    recording_pool = None
    recording_id_list = []

    try:
        # Set 120 second length, starting 5 seconds in future.
        start_time = utils.get_formatted_time(constants.SECONDS * 5, TimeFormat.TIME_FORMAT_MS,
                                              stream)
        end_time = utils.get_formatted_time(constants.SECONDS * 125, TimeFormat.TIME_FORMAT_MS,
                                            stream)

        # Create recording request
        LOGGER.info("CREATE RECORDING OBJECT INSTANCE")
        recording = recording_model.Recording(total_recordings=total_recording,
                                              StartTime=start_time, EndTime=end_time,
                                              StreamId=stream, copyType=copy_type)
        for i in range(total_recording):
            recording_id = recording.get_entry(i).RecordingId
            recording_id_list.append(recording_id)
            web_service_objects.append(notification_utils.get_web_service_object(recording_id))
            recording.get_entry(i).UpdateUrl = web_service_objects[i].get_url()
        LOGGER.debug("Recording instance created=%s", recording.serialize())

        # Send recording request to a8
        LOGGER.info("SEND ORIGINAL RECORDING REQUESTS TO A8")
        response = a8.create_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(
            response, requests.codes.no_content)
        LOGGER.info("Requested recordings from a8 - valid: %s, error: %s", is_valid, error)
        assert is_valid, error

        # Wait for all recordings to start
        LOGGER.info("WAIT FOR RECORDINGS TO START")
        for i in range(total_recording):
            is_valid, error = validate_recordings.validate_notification(
                web_service_objects[i], constants.RecordingStatus.STARTED)
            assert is_valid, error

        LOGGER.info("Current time=%s", datetime.datetime.utcnow())
        # Change the end times in the recording object instance to one second prior to current time
        new_end_time = utils.get_formatted_time(
            constants.SECONDS * -1, TimeFormat.TIME_FORMAT_MS, stream)
        LOGGER.info("UPDATE END TIMES IN RECORDING OBJECT INSTANCE")
        for i in range(total_recording):
            response = rio.find_recording(recording_id_list[i]).json()
            original_end_time = response[0][RecordingAttribute.END_TIME]
            recording.get_entry(i).EndTime = new_end_time
            LOGGER.info("%s end time - original=%s, updated=%s", recording_id_list[i],
                        original_end_time, new_end_time)

        # Send a8 request to update the recording end times
        LOGGER.info("SEND UPDATED RECORDING REQUESTS TO A8")
        response = a8.create_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(
            response, requests.codes.no_content)
        LOGGER.info("Requested updated recordings in a8 valid=%s, error=%s", is_valid, error)
        assert is_valid, error

        # Verify that the updated end time was populated
        LOGGER.info("VERIFY CHANGED END TIMES FOR RECORDINGS")
        for i in range(total_recording):
            response = rio.find_recording(recording_id_list[i]).json()
            is_valid, error = validate_recordings.validate_time(
                response, recording.get_entry(i).EndTime, RecordingAttribute.END_TIME)
            assert is_valid, error

        # The recordings should end after the end time changed
        LOGGER.info("VERIFY RECORDING COMPLETION")
        queue = Queue.Queue()
        recording_pool = mp_pool.ThreadPool(processes=total_recording)
        for i in range(total_recording):
            recording_pool.apply_async(validate_recordings.validate_notification,
                                       (web_service_objects[i], constants.RecordingStatus.COMPLETE,
                                        30), callback=queue.put)
        for i in range(total_recording):
            is_valid, error = queue.get()
            LOGGER.info("Recording %s complete valid=%s, error=%s", recording_id_list[i],
                        is_valid, error)
            assert is_valid, error

    finally:
        if recording_pool:
            recording_pool.close()
            recording_pool.join()
        if len(web_service_objects):
            for web_service_obj in web_service_objects:
                web_service_obj.stop_server()
        if recording:
            response = a8.delete_recording(recording)
            LOGGER.debug("Recording clean up status code=%s", response.status_code)
