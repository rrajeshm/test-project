import time
import Queue
import logging
import multiprocessing.pool as mp_pool
import os
import pytest
import requests

import helpers.utils as utils
import helpers.constants as constants
import helpers.vmr.rio.api as rio
import helpers.vmr.a8.a8_helper as a8
import helpers.models.recording as recording_model
import helpers.vmr.archive_helper as archive_helper
import validators.validate_common as validate_common
import helpers.notification.utils as notification_utils
import validators.validate_storage as validate_storage
import validators.validate_recordings as validate_recordings

from helpers.constants import Cos
from helpers.constants import Stream
from helpers.constants import Archive
from helpers.constants import TestLog
from helpers.constants import TimeFormat
from helpers.constants import ValidationError
from helpers.constants import RecordingAttribute, PlaybackTypes

pytestmark = pytest.mark.archival
LOGGER = logging.getLogger(os.environ.get("PYTEST_XDIST_WORKER",TestLog.TEST_LOGGER))


@utils.test_case_logger
def test_rtc9782_tc_arc_012_playback_after_archival_delete_during_playback(stream):
    """
    Create a recording with unique copy, wait until archiving completes, playback the recording and
    delete the recording during playback
    """
    recording = None
    web_service_obj = None

    try:
        start_time = utils.get_formatted_time(constants.SECONDS * 30, TimeFormat.TIME_FORMAT_MS, stream)
        end_time = utils.get_formatted_time(constants.SECONDS * 60, TimeFormat.TIME_FORMAT_MS, stream)
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

        vmr_plybk_url = utils.get_vmr_playback_url(recording_id=recording_id)
        print "[INFO: ] vmr playback url ", vmr_plybk_url
        archive_helper.wait_for_archival(stream, recording_id, Archive.ARCHIVE, Archive.COMPLETE)
        response = rio.find_recording(recording_id).json()
        print "[INFO: ] recording details ", response
        is_valid, error = validate_storage.validate_recording_in_storage(response, Cos.ARCHIVE_STORAGE,
                                                                         Cos.RECORDING_STORED)

        assert is_valid, error

        queue = Queue.Queue()

        playback_pool = mp_pool.ThreadPool(processes=1)
        playback_pool.apply_async(validate_recordings.validate_playback_using_vle, (recording_id,), callback=queue.put)

        #time.sleep(10 * constants.SECONDS)

        is_valid, error = validate_storage.validate_recording_in_storage(response, Cos.RECON_STORAGE,
                                                                         Cos.RECORDING_STORED)

        assert is_valid, ValidationError.SEGMENT_NOT_MOVED.format(Cos.RECON_STORAGE, response)

        del_resp = a8.delete_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(del_resp, requests.codes.no_content)
        assert is_valid, error

        is_valid, error = validate_recordings.validate_recording_deletion(recording_id)
        assert is_valid, error

        is_valid, error = queue.get()
        assert is_valid, error

        if is_valid:
            # verifying whether the recording available in VMR
            vmr_plybk_url = utils.get_vmr_playback_url(recording_id=recording_id)
            resp = requests.get(vmr_plybk_url)
            assert not (resp.status_code == requests.codes.ok), ValidationError.INCORRECT_HTTP_RESPONSE_STATUS_CODE.format(
                resp.status_code, resp.reason, resp.url)

    finally:
        web_service_obj.stop_server()
        response = a8.delete_recording(recording)
        LOGGER.debug("Recording clean up status code=%s", response.status_code)
