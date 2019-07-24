import datetime
import logging
import os
import pytest
import requests

import helpers.constants as constants
import helpers.models.recording as recording_model
import helpers.notification.utils as notification_utils
import helpers.utils as utils
import helpers.vmr.a8.a8_helper as a8
import helpers.vmr.rio.api as rio
import validators.validate_common as validate_common
import validators.validate_recordings as validate_recordings
import validators.validate_storage as validate_storage
from helpers.constants import Cos
from helpers.constants import RecordingAttribute
from helpers.constants import TestLog
from helpers.constants import TimeFormat

pytestmark = pytest.mark.recording
LOGGER = logging.getLogger(os.environ.get("PYTEST_XDIST_WORKER",TestLog.TEST_LOGGER))


@utils.test_case_logger
def test_rtc9751_tc_rec_003_past_start_time_future_end_time(stream):
    """
    Create a recording with past start time and future end time
    """
    recording = None
    web_service_obj = None

    try:
        start_time = utils.get_formatted_time(constants.SECONDS * -60, TimeFormat.TIME_FORMAT_MS, stream)
        end_time = utils.get_formatted_time(constants.SECONDS * 60, TimeFormat.TIME_FORMAT_MS, stream)
        recording = recording_model.Recording(StartTime=start_time, EndTime=end_time, StreamId=stream)
        recording_id = recording.get_entry(0).RecordingId
        web_service_obj = notification_utils.get_web_service_object(recording_id)
        recording.get_entry(0).UpdateUrl = web_service_obj.get_url()

        LOGGER.debug("Recording instance created=%s", recording.serialize())
        response = a8.create_recording(recording)
        minimum_start_time = datetime.datetime.utcnow() + datetime.timedelta(0, constants.MINIMUM_SEGMENT_TIME_DIFF)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)

        assert is_valid, error

        is_valid, status = validate_recordings.validate_recording(recording_id, web_service_obj, minimum_start_time)

        assert is_valid, status

        if status == constants.RecordingStatus.COMPLETE:
            is_valid, error = validate_recordings.validate_playback(recording_id)

            assert is_valid, error
    finally:
        web_service_obj.stop_server()
        response = a8.delete_recording(recording)
        LOGGER.debug("Recording clean up status code=%s", response.status_code)
