import logging
import pytest
import requests
import os
import yaml

import helpers.constants as constants
import helpers.models.recording as recording_model
import helpers.notification.utils as notification_utils
import helpers.utils as utils
import helpers.vmr.a8.a8_helper as a8
import helpers.vmr.rio.api as rio
import helpers.vmr.archive_helper as archive_helper
import validators.validate_common as validate_common
import validators.validate_recordings as validate_recordings
import validators.validate_storage as validate_storage
from helpers.constants import Component
from helpers.constants import Cos
from helpers.constants import RecordingAttribute
from helpers.constants import TestLog
from helpers.constants import TimeFormat
from helpers.constants import Archive

pytestmark = pytest.mark.recording
LOGGER = logging.getLogger(os.environ.get("PYTEST_XDIST_WORKER",TestLog.TEST_LOGGER))
# CONFIG_INFO = yaml.load(pytest.config.getoption(constants.GLOBAL_CONFIG_INFO))
generic_conf = utils.get_spec_config()

TC_sanity_hybrid_copy_DATA = [("sanity_rtc10067_hybrid_common", RecordingAttribute.COPY_TYPE_COMMON),
                              ("sanity_rtc10066_hybrid_unique", RecordingAttribute.COPY_TYPE_UNIQUE)]


@utils.test_case_logger
@pytest.mark.parametrize("name, copy_type", TC_sanity_hybrid_copy_DATA, ids=[x[0] for x in TC_sanity_hybrid_copy_DATA])
def test_sanity_hybrid_copy(stream, name, copy_type):
    """
    Schedule UNIQUE and COMMON copy on same channel and verify the recording and playback.
    """
    recording = None
    web_service_obj = None
    try:
        start_time = utils.get_formatted_time(constants.SECONDS * 30, TimeFormat.TIME_FORMAT_MS, stream)
        end_time = utils.get_formatted_time(constants.SECONDS * 80, TimeFormat.TIME_FORMAT_MS, stream)
        recording = recording_model.Recording(StartTime=start_time, EndTime=end_time, StreamId=stream,
                                              copyType=copy_type)
        recording_id = recording.get_entry(0).RecordingId
        web_service_obj = notification_utils.get_web_service_object(recording_id)
        recording.get_entry(0).UpdateUrl = web_service_obj.get_url()
        LOGGER.debug("Recording instance created=%s", recording.serialize())
        response = a8.create_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)
        assert is_valid, error

        is_valid, error = validate_recordings.validate_recording(recording_id, web_service_obj)
        assert is_valid, error

        archive_helper.wait_for_archival(stream, recording_id, Archive.ARCHIVE, Archive.COMPLETE)
        response = rio.find_recording(recording_id).json()
        is_valid, error = validate_storage.validate_recording_in_storage(response, Cos.ACTIVE_STORAGE,
                                                                         Cos.RECORDING_NOT_STORED)
        assert is_valid, error

        is_valid, error = validate_storage.validate_recording_in_storage(response, Cos.ARCHIVE_STORAGE,
                                                                         Cos.RECORDING_STORED)
        assert is_valid, error

        is_valid, error = validate_recordings.validate_playback(recording_id)
        assert is_valid, error
    finally:
        web_service_obj.stop_server()
        response = a8.delete_recording(recording)
        LOGGER.debug("Recording clean up status code=%s", response.status_code)
