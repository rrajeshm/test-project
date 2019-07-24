import logging
import os
import pytest
import requests
import yaml

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
from helpers.constants import Stream
from helpers.constants import TestLog
from helpers.constants import TimeFormat
from helpers.constants import ValidationError
from helpers.constants import Vle
from helpers.vle import vle_validators_configuration
from helpers.constants import Component

pytestmark = pytest.mark.playback
LOGGER = logging.getLogger(os.environ.get("PYTEST_XDIST_WORKER",TestLog.TEST_LOGGER))


TC_PLY_001_002_003_DATA = [("rtc9792_001_multiple_bitrate_single_client_smart", Vle.STREAM_STYLE_ROBIN),
                           ("rtc9793_002_multiple_bitrate_single_client_random", Vle.STREAM_STYLE_RANDOM),
                           ("rtc9794_003_single_bitrate_single_client_all", Vle.STREAM_STYLE_DEFAULT)]


@utils.test_case_logger
@pytest.mark.parametrize("name, stream_style", TC_PLY_001_002_003_DATA,
                         ids=[x[0] for x in TC_PLY_001_002_003_DATA])
def test_tc_ply_stream_style(name, stream, stream_style):
    """
    Playback vle using multiple/single bit rates and using different stream styles
    """
    recording = None
    web_service_obj = None

    try:
        start_time = utils.get_formatted_time(constants.SECONDS * 30, TimeFormat.TIME_FORMAT_MS, stream)
        end_time = utils.get_formatted_time(constants.SECONDS * 60, TimeFormat.TIME_FORMAT_MS, stream)
        copy_type = RecordingAttribute.COPY_TYPE_UNIQUE
        recording = recording_model.Recording(StartTime=start_time, EndTime=end_time, copyType=copy_type,
                                              StreamId=stream)
        recording_id = recording.get_entry(0).RecordingId
        web_service_obj = notification_utils.get_web_service_object(recording_id)
        recording.get_entry(0).UpdateUrl = web_service_obj.get_url()
        LOGGER.debug("Recording instance created=%s", recording.serialize())
        response = a8.create_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)

        assert is_valid, error
        is_valid, error = validate_recordings.validate_recording(recording_id, web_service_obj)

        assert is_valid, error
        vle_request_params = {}
        LOGGER.debug("Configuring vle to playback with clients=%s and stream style=%s", 1, stream_style)
        vle_request_params[Vle.CLIENTS_NUM_MAX] = 1
        vle_request_params[Vle.STREAM_STYLE] = stream_style
        if Vle.STREAM_STYLE_DEFAULT == stream_style:
            is_valid, msg = utils.get_video_profiles_from_m3u8(recording_id)

            assert is_valid, msg

            # If Valid, msg will contain the list of video profiles
            assert msg, ValidationError.VIDEO_PROFILES_NOT_FOUND.format(recording_id)
            vle_request_params[Vle.DOWNLOAD_BITRATE] = msg[0]

        is_valid, error = validate_recordings.validate_playback(recording_id,
                                                                VLE_REQUEST_PARAMS=vle_request_params)
        assert is_valid, error
    finally:
        web_service_obj.stop_server()
        response = a8.delete_recording(recording)
        LOGGER.debug("Recording clean up status code=%s", response.status_code)
