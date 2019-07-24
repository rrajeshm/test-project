import time
import Queue
import logging
import datetime
import multiprocessing.pool as mp_pool
import os
import pytest
import requests

import helpers.utils as utils
import helpers.constants as constants
import helpers.vmr.rio.api as rio
import helpers.vmr.a8.a8_helper as a8
import helpers.models.recording as recording_model
import helpers.notification.utils as notification_utils
import validators.validate_common as validate_common
import validators.validate_recordings as validate_recordings
import validators.validate_storage as validate_storage

from helpers.constants import Cos
from helpers.constants import Stream
from helpers.constants import TestLog
from helpers.constants import TimeFormat
from helpers.constants import ValidationError
from helpers.constants import RecordingAttribute

pytestmark = pytest.mark.recording
LOGGER = logging.getLogger(os.environ.get("PYTEST_XDIST_WORKER",TestLog.TEST_LOGGER))

RECORDING_START_DELAY = [1, 30]

@utils.test_case_logger
@pytest.mark.parametrize("recording_start_delay", RECORDING_START_DELAY)
def test_rtc9758_tc_rec_012_IPDVRTESTS_52_delete_and_playback_after_recording_start(recording_start_delay, stream):
    """
    Create a recording, delete and play it back after it starts
    """
    recording = None
    web_service_obj = None

    try:
        start_time = utils.get_formatted_time(constants.SECONDS * recording_start_delay, TimeFormat.TIME_FORMAT_MS, stream)
        end_time = utils.get_formatted_time(constants.SECONDS * 60, TimeFormat.TIME_FORMAT_MS, stream)
        recording = recording_model.Recording(StartTime=start_time, EndTime=end_time, StreamId=stream)
        recording_id = recording.get_entry(0).RecordingId
        web_service_obj = notification_utils.get_web_service_object(recording_id)
        recording.get_entry(0).UpdateUrl = web_service_obj.get_url()

        LOGGER.debug("Recording instance created=%s", recording.serialize())
        response = a8.create_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)

        assert is_valid, error

        recording_id = recording.get_entry(0).RecordingId
        response = rio.find_recording(recording_id).json()
        LOGGER.debug("Response=%s", response)
        start_time = utils.get_parsed_time(response[0][RecordingAttribute.START_TIME][:-1])
        current_time = datetime.datetime.utcnow()

        # wait till the recording start time
        if current_time < start_time:
            utils.wait(start_time - current_time, constants.TIME_DELTA)
        is_valid, error = validate_recordings.validate_notification(web_service_obj, constants.RecordingStatus.STARTED)

        assert is_valid, error

        time.sleep(15 * constants.SECONDS)
        delete_time = utils.get_formatted_time(constants.SECONDS * 0, TimeFormat.TIME_FORMAT_MS, stream)
        response = a8.delete_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)

        assert is_valid, error

        is_valid, error = validate_recordings.validate_recording_deletion(recording_id)

        assert is_valid, error

        is_valid, error = validate_recordings.validate_playback_using_vle(recording_id, EndTime=delete_time)

        assert not is_valid, ValidationError.DELETED_RECORDING_PLAYED_BACK.format(recording_id)
    finally:
        web_service_obj.stop_server()
        response = a8.delete_recording(recording)
        LOGGER.debug("Recording clean up status code=%s", response.status_code)
