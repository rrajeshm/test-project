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
from helpers.constants import Component
from helpers.constants import TestLog
from helpers.constants import TimeFormat
from helpers.constants import ValidationError
from helpers.constants import RecordingAttribute

pytestmark = pytest.mark.recording
LOGGER = logging.getLogger(os.environ.get("PYTEST_XDIST_WORKER",TestLog.TEST_LOGGER))

@utils.test_case_logger
def test_delete_hot_recording_ipdvrtests_52(stream):
    """
    JIRA ID : IPDVRTESTS-52
    TITLE   : "Delete Rec(Hot recording)"
    STEPS   : Create a 30 min recording, when program is already started, mark the recording for deletion.
              Check memsql to verify erase time is set.
              Check cos http logs for delete segments.
              Playback should not be possible after marking it for delete.
    """
    recording = None
    web_service_obj = None

    try:
        rec_duration = utils.get_rec_duration(dur_confg_key=Component.LARGE_REC_LEN_IN_SEC)
        start_time = utils.get_formatted_time(constants.SECONDS, TimeFormat.TIME_FORMAT_MS, stream)
        end_time = utils.get_formatted_time((constants.SECONDS * rec_duration),
                                            TimeFormat.TIME_FORMAT_MS, stream)
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
        response = rio.find_recording(recording_id).json()
        is_valid, error = validate_storage.validate_recording_in_storage(response, Cos.ACTIVE_STORAGE,
                                                                         Cos.RECORDING_NOT_STORED)
        assert is_valid, error

        is_valid, error = validate_recordings.validate_playback_using_vle(recording_id, EndTime=delete_time)

        assert not is_valid, ValidationError.DELETED_RECORDING_PLAYED_BACK.format(recording_id)
    finally:
        if web_service_obj:
            web_service_obj.stop_server()
        if recording:
            response = a8.delete_recording(recording)
            LOGGER.debug("Recording clean up status code=%s", response.status_code)
