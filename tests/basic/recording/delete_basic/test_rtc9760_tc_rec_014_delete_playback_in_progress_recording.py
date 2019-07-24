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

TOTAL_RECORDINGS_LIST = [1, 20]
TC_REC_038_013_DATA = [("rtc9770_bulk_delete_active_assets", 20),
                       ("rtc9759_delete_after_recording_complete", 1)]


@utils.test_case_logger
def test_rtc9760_tc_rec_014_delete_playback_in_progress_recording(stream):
    """
    Delete a recording while the playback is in progress and verify the playback
    """
    recording = None
    playback_pool = None
    web_service_obj = None

    try:
        start_time = utils.get_formatted_time(constants.SECONDS * 30, TimeFormat.TIME_FORMAT_MS, stream)
        end_time = utils.get_formatted_time(constants.SECONDS * 150, TimeFormat.TIME_FORMAT_MS, stream)
        recording = recording_model.Recording(StartTime=start_time, EndTime=end_time, StreamId=stream)
        recording_id = recording.get_entry(0).RecordingId
        web_service_obj = notification_utils.get_web_service_object(recording_id)
        recording.get_entry(0).UpdateUrl = web_service_obj.get_url()
        LOGGER.debug("Recording instance created=%s", recording.serialize())
        response = a8.create_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)

        assert is_valid, error

        is_valid, error = validate_recordings.validate_recording(recording_id, web_service_obj)

        assert is_valid, error

        queue = Queue.Queue()

        playback_pool = mp_pool.ThreadPool(processes=1)
        playback_pool.apply_async(validate_recordings.validate_playback_using_vle, (recording_id,), callback=queue.put)

        time.sleep(10 * constants.SECONDS)
        response = a8.delete_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)

        assert is_valid, error

        is_valid, error = validate_recordings.validate_recording_deletion(recording_id)

        assert is_valid, error

        is_valid, error = queue.get()

        if is_valid:
            # verifying whether the playback fails now, even though it succeeded previously
            is_valid, error = validate_recordings.validate_playback_using_vle(recording_id)

        assert not is_valid, ValidationError.DELETED_RECORDING_PLAYED_BACK.format(recording_id)
    finally:
        if playback_pool:
            playback_pool.close()
            playback_pool.join()
        web_service_obj.stop_server()
        response = a8.delete_recording(recording)
        LOGGER.debug("Recording clean up status code=%s", response.status_code)
