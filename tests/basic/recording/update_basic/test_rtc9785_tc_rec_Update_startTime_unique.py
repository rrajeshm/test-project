import Queue
import datetime
import logging
import time
import multiprocessing.pool as mp_pool
import os
import pytest
import requests

import helpers.constants as constants
import helpers.models.recording as recording_model
import helpers.notification.utils as notification_utils
import helpers.utils as utils
import helpers.vmr.a8.a8_helper as a8
import helpers.vmr.archive_helper as archive_helper
import helpers.vmr.rio.api as rio
import validators.validate_common as validate_common
import validators.validate_storage as validate_storage
import validators.validate_recordings as validate_recordings

from helpers.constants import Cos
from helpers.constants import Archive
from helpers.constants import RecordingAttribute
from helpers.constants import RecordingStatus
from helpers.constants import TestLog
from helpers.constants import TimeFormat
from helpers.constants import Stream

pytestmark = pytest.mark.recording
LOGGER = logging.getLogger(os.environ.get("PYTEST_XDIST_WORKER",TestLog.TEST_LOGGER))

TOTAL_RECORDINGS_LIST = [20]

TC_REC_DATA = [("rtc9785_update_start_time_before_recording_start_unique", RecordingAttribute.COPY_TYPE_UNIQUE)]

@utils.test_case_logger
@pytest.mark.parametrize("total_recording", TOTAL_RECORDINGS_LIST)
@pytest.mark.parametrize("name, copy_type", TC_REC_DATA)

def test_tc_rec_(total_recording, stream, name, copy_type):
    """
    Update the start time of a recording before it starts for both unique/ common
    """
    recording = None
    web_service_objects = []
    recording_pool = None
    recording_id_list = []

    try:
        queue = Queue.Queue()
        start_time = utils.get_formatted_time(constants.SECONDS * 30, TimeFormat.TIME_FORMAT_MS, stream)
        end_time = utils.get_formatted_time(constants.SECONDS * 70, TimeFormat.TIME_FORMAT_MS, stream)
        recording = recording_model.Recording(total_recordings=total_recording, StartTime=start_time, EndTime=end_time,
                                              StreamId=stream, copyType=copy_type)

        for i in range(total_recording):
            recording_id = recording.get_entry(i).RecordingId
            recording_id_list.append(recording_id)
            web_service_objects.append(notification_utils.get_web_service_object(recording_id))
            recording.get_entry(i).UpdateUrl = web_service_objects[i].get_url()
        LOGGER.debug("Recording instance created=%s", recording.serialize())
        response = a8.create_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)

        assert is_valid, error
        recording_pool = mp_pool.ThreadPool(processes=total_recording)
        for i in range(total_recording):
            response = rio.find_recording(recording_id_list[i]).json()
            LOGGER.debug("Response=%s", response)
            start_time = response[0][RecordingAttribute.START_TIME][:-1]
            LOGGER.debug("Scheduled start time of recording=%s is %s", recording_id_list[i], start_time)
            recording.get_entry(i).StartTime = utils.get_formatted_time(constants.SECONDS * 40,
                                                                        TimeFormat.TIME_FORMAT_MS, stream)
            LOGGER.debug("Updated start time of recording=%s to %s", recording_id_list[i],
                         recording.get_entry(i).StartTime)

        # Update the recording with the updated start time
        response = a8.create_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)

        assert is_valid, error

        # Validating whether the updated start time was populated or not
        for i in range(total_recording):
            response = rio.find_recording(recording_id_list[i]).json()
            LOGGER.debug("Response=%s", response)
            is_valid, error = validate_recordings.validate_time(response, recording.get_entry(i).StartTime,
                                                                RecordingAttribute.START_TIME)

            assert is_valid, error

        for i in range(total_recording):
            recording_pool.apply_async(validate_recordings.validate_recording,
                                       (recording.get_entry(i).RecordingId, web_service_objects[i]),
                                       callback=queue.put)
        for i in range(total_recording):
            is_valid, error = queue.get()
            assert is_valid, error

    finally:
        if recording_pool:
            recording_pool.close()
            recording_pool.join()
        for web_service_obj in web_service_objects:
            web_service_obj.stop_server()
        response = a8.delete_recording(recording)
        LOGGER.debug("Recording clean up status code=%s", response.status_code)
