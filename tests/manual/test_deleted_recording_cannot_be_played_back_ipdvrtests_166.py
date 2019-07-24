import time
import Queue
import logging
import datetime
import multiprocessing.pool as mp_pool
import os
import pytest
import requests
import yaml

import helpers.utils as utils
import helpers.constants as constants
import helpers.vmr.rio.api as rio
import helpers.vmr.a8.a8_helper as a8
import helpers.models.recording as recording_model
import helpers.notification.utils as notification_utils
import validators.validate_common as validate_common
import validators.validate_recordings as validate_recordings
import validators.validate_storage as validate_storage

from helpers.constants import Cos, Component
from helpers.constants import Stream
from helpers.constants import TestLog
from helpers.constants import TimeFormat
from helpers.constants import ValidationError
from helpers.constants import RecordingAttribute

pytestmark = pytest.mark.recording
LOGGER = utils.get_logger()


def test_deleted_recording_cannot_be_played_back_ipdvrtests_166(common_lib, stream):
    """
    JIRA ID : IPDVRTESTS-166
    JIRA LINK : https://jira01.engit.synamedia.com/browse/IPDVRTESTS-166
    """
    recording = None
    web_service_obj = None

    try:
        # Step1: Create a 30 minute recording
        recording, web_service_obj = common_lib.create_recording(stream, rec_duration = Component.LARGE_REC_LEN_IN_SEC)
        recording_id = recording.get_entry(0).RecordingId        
        is_valid, error = validate_recordings.validate_recording(recording_id, web_service_obj)
        assert is_valid, error
        
        # Step2: Delete the recording 
        response = a8.delete_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)
        assert is_valid, error
        is_valid, error = validate_recordings.validate_recording_deletion(recording_id)
        assert is_valid, error
        
        # Step 3: Validating playback
        is_valid, error = validate_recordings.validate_playback_using_vle(recording_id)
        assert not is_valid, ValidationError.DELETED_RECORDING_PLAYED_BACK.format(recording_id)

    finally:
        if web_service_obj: web_service_obj.stop_server()
        if recording:
            response = a8.delete_recording(recording)
            LOGGER.debug("Recording clean up status code=%s", response.status_code)
