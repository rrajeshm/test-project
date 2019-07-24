import logging
import time
import os
import pytest
import requests

import helpers.constants as constants
import helpers.models.recording as recording_model
import helpers.notification.utils as notification_utils
import helpers.utils as utils
import helpers.vmr.a8.a8_helper as a8
import validators.validate_common as validate_common
import validators.validate_recordings as validate_recordings
import validators.validate_streams as validate_streams
from helpers.constants import TestLog
from helpers.constants import TimeFormat
from helpers.constants import ValidationError

pytestmark = pytest.mark.recording
LOGGER = logging.getLogger(os.environ.get("PYTEST_XDIST_WORKER",TestLog.TEST_LOGGER))
# STREAM_ID_LIST, TC_NAME_IDS_LIST = utils.get_stream_ids_tc_ids_list()
STREAM_ID_LIST = utils.get_streams()

@utils.test_case_logger
def test_rtc9816_tc_rec_007_update_stream_id_before_recording_start(stream):
    """
    Create a recording and update the stream ID before the recording starts
    """
    recording = None
    web_service_obj = None

    try:
        start_time = utils.get_formatted_time(constants.SECONDS * 120, TimeFormat.TIME_FORMAT_MS, stream)
        end_time = utils.get_formatted_time(constants.SECONDS * 150, TimeFormat.TIME_FORMAT_MS, stream)
        recording = recording_model.Recording(StartTime=start_time, EndTime=end_time, StreamId=stream)
        recording_id = recording.get_entry(0).RecordingId
        web_service_obj = notification_utils.get_web_service_object(recording_id)
        recording.get_entry(0).UpdateUrl = web_service_obj.get_url()
        LOGGER.debug("Recording instance created=%s", recording.serialize())
        response = a8.create_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)

        assert is_valid, error

        time.sleep(constants.SECONDS * 60)
        stream_list_rem = [x for x in STREAM_ID_LIST if x != stream]
        assert stream_list_rem, ValidationError.STREAM_NOT_CONFIGURED
        recording.get_entry(0).StreamId = stream_list_rem[0]

        # Update the previously created recording with the new stream ID
        response = a8.create_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)

        assert is_valid, error

        is_valid, error = validate_streams.validate_stream_id(recording_id, stream_list_rem[0])

        assert is_valid, error

        is_valid, error = validate_recordings.validate_recording(recording_id, web_service_obj)

        assert is_valid, error

        is_valid, error = validate_recordings.validate_playback(recording_id)

        assert is_valid, error
    finally:
        web_service_obj.stop_server()
        response = a8.delete_recording(recording)
        LOGGER.debug("Recording clean up status code=%s", response.status_code)
