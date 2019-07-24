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
						   
TC_PLY_004_005_006_DATA = [("rtc9795_004_playback_audio_language_eng", Vle.AUDIO_LANG_ENG),
                           ("rtc9796_005_playback_audio_language_fre", Vle.AUDIO_LANG_FRE),
                           ("rtc9797_006_playback_audio_language_spa", Vle.AUDIO_LANG_SPA)]

@utils.test_case_logger
def test_rtc9799_tc_ply_008_playback_pause_resume(stream):
    """
    Create a recording, pause and resume during playback and verify if playback was successful
    """
    recording = None
    web_service_obj = None
    pause_trigger = 2
    pause_duration = 5

    try:
        start_time = utils.get_formatted_time(constants.SECONDS * 30, TimeFormat.TIME_FORMAT_MS, stream)
        end_time = utils.get_formatted_time(constants.SECONDS * 60, TimeFormat.TIME_FORMAT_MS, stream)
        copy_type = RecordingAttribute.COPY_TYPE_COMMON
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

        response = rio.find_recording(recording_id).json()
        is_valid, error = validate_storage.validate_recording_in_storage(response, Cos.ACTIVE_STORAGE,
                                                                         Cos.RECORDING_STORED)

        assert is_valid, error

        is_valid, error = validate_storage.validate_recording_in_storage(response, Cos.ACTIVE_STORAGE,
                                                                         Cos.RECORDING_NOT_STORED, 1)

        assert is_valid, error

        vle_request_params = {Vle.TRICKMODES: '{0},{1},{2},{3}'.format(Vle.TRICKMODE_PAUSE, pause_trigger,
                                                                       pause_duration, Vle.PAUSE_WRT_SEGMENT)}
        is_valid, error = validate_recordings.validate_playback(recording_id, VLE_REQUEST_PARAMS=vle_request_params,
                                                                VALIDATOR_TYPE=vle_validators_configuration.PLAYBACK_VALIDATION_TRICKMODE)
        assert is_valid, error
    finally:
        web_service_obj.stop_server()
        response = a8.delete_recording(recording)
        LOGGER.debug("Recording clean up status code=%s", response.status_code)
