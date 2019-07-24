import Queue
import logging
import multiprocessing.pool as mp_pool
import os
import pytest
import requests
import time

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

TC9788_DATA = [("test_rtc9788_bulk_update_end_time_during_recording_inprogress_unique",
                RecordingAttribute.COPY_TYPE_UNIQUE)]


@utils.test_case_logger
@pytest.mark.parametrize("total_recording", TOTAL_RECORDINGS_LIST)
@pytest.mark.parametrize("name, copy_type", TC9788_DATA, ids=[x[0] for x in TC9788_DATA])
def test_rtc9788_bulk_update_end_time_during_recording_inprogress_unique(total_recording, stream, name, copy_type):
    """
    TC9788 : Bulk update of recording requests of end time during recording(20 requests ) unique copy
    """
    recording = None
    web_service_objects = []
    recording_pool = None
    recording_id_list = []

    try:
        queue = Queue.Queue()
        start_time = utils.get_formatted_time(constants.SECONDS * 30, TimeFormat.TIME_FORMAT_MS, stream)
        end_time = utils.get_formatted_time(constants.SECONDS * 180, TimeFormat.TIME_FORMAT_MS, stream)
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

        start_wait = constants.RECORDING_DELAY + 30
        sleep_time = utils.get_sleep_time(start_wait, stream)
        print "[INFO: ] Waiting %d seconds for recording to start" % sleep_time
        LOGGER.debug("Waiting %d seconds for recording to start", sleep_time)
        time.sleep(sleep_time)

        for i in range(total_recording):
            response = rio.find_recording(recording_id_list[i]).json()
            LOGGER.debug("Response=%s", response)
            end_time = response[0][RecordingAttribute.END_TIME][:-1]

            LOGGER.debug("Scheduled end time of recording=%s is %s", recording_id_list[i], end_time)
            recording.get_entry(i).EndTime = utils.get_formatted_time(constants.SECONDS * 70,
                                                                      TimeFormat.TIME_FORMAT_MS, stream)

            LOGGER.debug("Updated end time of recording=%s to %s", recording_id_list[i], recording.get_entry(i).EndTime)

        # Update the recording with the updated end time
        response = a8.create_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)
        assert is_valid, error

        # Validating whether the updated end time was populated or not
        for i in range(total_recording):
            response = rio.find_recording(recording_id_list[i]).json()
            LOGGER.debug("Response=%s", response)
            is_valid, error = validate_recordings.validate_time(response, recording.get_entry(i).EndTime,
                                                                RecordingAttribute.END_TIME)
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
        if len(web_service_objects):
            for web_service_obj in web_service_objects:
                web_service_obj.stop_server()
        if recording:
            response = a8.delete_recording(recording)
            LOGGER.debug("Recording clean up status code=%s", response.status_code)
