import logging
import pytest
import requests

import helpers.utils as utils
import helpers.constants as constants
import helpers.vmr.rio.api as rio
import helpers.vmr.a8.a8_helper as a8
import helpers.vmr.archive_helper as archive_helper
import helpers.models.recording as recording_model
import helpers.notification.utils as notification_utils
import validators.validate_common as validate_common
import validators.validate_storage as validate_storage
import validators.validate_recordings as validate_recordings

from helpers.constants import Cos
from helpers.constants import TestLog
from helpers.constants import Archive
from helpers.constants import TimeFormat
from helpers.constants import RecordingAttribute
from helpers.constants import Component

pytestmark = pytest.mark.recording
LOGGER = logging.getLogger(TestLog.TEST_LOGGER)

# CONFIG_INFO = yaml.load(pytest.config.getoption(constants.GLOBAL_CONFIG_INFO))
generic_conf = utils.get_spec_config()
private_copy_stream = generic_conf[Component.PRIVATE_COPY_STREAM][Component.STREAM_1][Component.ID] if generic_conf.get(
    Component.PRIVATE_COPY_STREAM) else None

TC_ER_019_DATA = [("private_copy_archiving_common", RecordingAttribute.COPY_TYPE_COMMON),
                  ("private_copy_archiving_unique", RecordingAttribute.COPY_TYPE_UNIQUE)]


@utils.test_case_logger
@pytest.mark.parametrize("name, copy_type", TC_ER_019_DATA, ids=[x[0] for x in TC_ER_019_DATA])
@pytest.mark.skipif(not generic_conf.get(
    Component.PRIVATE_COPY_STREAM), reason = "Configuration doesn't have private copy stream")
def test_tc_er_019(stream, name, copy_type):
    recording = None
    web_service_obj = None

    try:
        start_time = utils.get_formatted_time(constants.SECONDS * 30, TimeFormat.TIME_FORMAT_MS, stream)
        end_time = utils.get_formatted_time(constants.SECONDS * 60, TimeFormat.TIME_FORMAT_MS, stream)
        recording = recording_model.Recording(StartTime=start_time, EndTime=end_time,
                                              StreamId=stream, copyType=copy_type)
        recording_id = recording.get_entry(0).RecordingId
        web_service_obj = notification_utils.get_web_service_object(recording_id)
        recording.get_entry(0).UpdateUrl = web_service_obj.get_url()
        LOGGER.debug("Recording instance created=%s", recording.serialize())
        response = a8.create_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)
        assert is_valid, error

        is_valid, error = validate_recordings.validate_recording(recording_id, web_service_obj)
        assert is_valid, error

        response = rio.find_recording(recording_id).json()
        is_valid, error = validate_storage.validate_recording_in_storage(response, Cos.ACTIVE_STORAGE,
                                                                         Cos.RECORDING_STORED)
        assert is_valid, error

        if copy_type == RecordingAttribute.COPY_TYPE_UNIQUE:
            archive_helper.wait_for_archival(stream, recording_id, Archive.ARCHIVE, Archive.IN_PROGRESS)
            is_valid, error = validate_storage.validate_recording_in_storage(response, Cos.ARCHIVE_STORAGE,
                                                                         Cos.RECORDING_NOT_STORED)
            assert is_valid, error
    finally:
        if web_service_obj:
            web_service_obj.stop_server()
        if recording:
            response = a8.delete_recording(recording)
            LOGGER.debug("Recording clean up status code=%s", response.status_code)
