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


@utils.test_case_logger
def test_rtc9757_tc_rec_011_delete_before_recording_start(stream):
    """
    Create a recording and delete it before it starts
    """
    start_time = utils.get_formatted_time(constants.SECONDS * 120, TimeFormat.TIME_FORMAT_MS, stream)
    end_time = utils.get_formatted_time(constants.SECONDS * 240, TimeFormat.TIME_FORMAT_MS, stream)
    recording = recording_model.Recording(StartTime=start_time, EndTime=end_time, StreamId=stream)
    LOGGER.debug("Recording instance created=%s", recording.serialize())
    response = a8.create_recording(recording)
    is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)

    assert is_valid, error

    time.sleep(constants.SECONDS * 60)
    response = a8.delete_recording(recording)
    is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)

    assert is_valid, error

    recording_id = recording.get_entry(0).RecordingId
    is_valid, error = validate_recordings.validate_recording_deletion(recording_id)

    assert is_valid, error
