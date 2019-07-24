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

TOTAL_RECORDINGS_LIST = [1, 20]

TC_REC_008_034_DATA = [("rtc9766_update_start_time_before_recording_start_common", RecordingAttribute.COPY_TYPE_COMMON),
                       ("rtc9754_update_start_time_before_recording_start_unique", RecordingAttribute.COPY_TYPE_UNIQUE)]
					   
TC_REC_009_036_DATA = [("rtc9767_update_end_time_before_recording_start_common", RecordingAttribute.COPY_TYPE_COMMON),
                       ("rtc9755_update_end_time_before_recording_start_unique", RecordingAttribute.COPY_TYPE_UNIQUE)]
					   
					   
TC_REC_037_DATA = [("update_end_time_after_recording_start_common", RecordingAttribute.COPY_TYPE_COMMON),
                   ("update_end_time_after_recording_start_unique", RecordingAttribute.COPY_TYPE_UNIQUE)]



@utils.test_case_logger
def test_rtc9756_tc_rec_010_recording_stop_after_recording_start(stream):
    """
    Create a recording and once it starts, stop it by setting the end time as the current time
    """
    recording = None
    web_service_obj = None

    try:
        start_time = utils.get_formatted_time(constants.SECONDS * 30, TimeFormat.TIME_FORMAT_MS, stream)
        end_time = utils.get_formatted_time(constants.SECONDS * 150, TimeFormat.TIME_FORMAT_MS, stream)
        recording = recording_model.Recording(StartTime=start_time, EndTime=end_time, StreamId=stream)
        recording_id = recording.get_entry(0).RecordingId
        web_service_obj = notification_utils.get_web_service_object(recording_id)
        recording.get_entry(0).UpdateUrl = web_service_obj.get_url()

        LOGGER.debug("Recording instance created=%s", recording.serialize())
        response = a8.create_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)

        assert is_valid, error

        response = rio.find_recording(recording_id).json()
        LOGGER.debug("Response=%s", response)
        start_time = utils.get_parsed_time(response[0][RecordingAttribute.START_TIME][:-1])
        current_time = datetime.datetime.utcnow()

        # wait till the recording start time
        if current_time < start_time:
            utils.wait(start_time - current_time, constants.TIME_DELTA)
        is_valid, error = validate_recordings.validate_notification(web_service_obj, constants.RecordingStatus.STARTED)

        assert is_valid, error

        time.sleep(constants.SECONDS * 60)
        recording.get_entry(0).EndTime = utils.get_formatted_time(constants.SECONDS * 0, TimeFormat.TIME_FORMAT_MS, stream)
        response = a8.create_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)

        assert is_valid, error

        time.sleep(constants.SECONDS * 4)
        is_valid, error = validate_recordings.validate_recording_end_state(recording_id, [RecordingStatus.COMPLETE],
                                                                           web_service_obj=web_service_obj)
        assert is_valid, error
    finally:
        web_service_obj.stop_server()
        response = a8.delete_recording(recording)
        LOGGER.debug("Recording clean up status code=%s", response.status_code)
