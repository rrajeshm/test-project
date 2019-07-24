import logging
import os
import pytest
import requests

import helpers.constants as constants
import helpers.models.recording as recording_model
import helpers.utils as utils
import helpers.vmr.a8.a8_helper as a8
import validators.validate_common as validate_common
import validators.validate_recordings as validate_recordings
from helpers.constants import RecordingError
from helpers.constants import TestLog

pytestmark = pytest.mark.recording
LOGGER = logging.getLogger(os.environ.get("PYTEST_XDIST_WORKER",TestLog.TEST_LOGGER))


@utils.test_case_logger
def test_rtc9823_tc_rec_020_invalid_scheduled_time(stream):
    """
    Schedule a recording with an invalid scheduled time
    """
    recording = recording_model.Recording(ScheduledTime=constants.INVALID_TIME, StreamId=stream)
    LOGGER.debug("Recording instance created=%s", recording.serialize())
    response = a8.create_recording(recording)
    is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.bad_request)

    assert is_valid, error

    is_valid, error = validate_recordings.validate_recording_error_code(response, RecordingError.INVALID_TIME)

    assert is_valid, error
