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
from helpers.constants import RecordingAttribute

pytestmark = pytest.mark.archival
LOGGER = logging.getLogger(os.environ.get("PYTEST_XDIST_WORKER",TestLog.TEST_LOGGER))

@utils.test_case_logger
def test_rtc9774_tc_arc_004_multiple_common(stream):
    """
    Create multiple recording with copy type as common and verify that the recording does not get archived
    """

    total_recordings = 3
    recording = None
    web_service_objs = []
    recording_pool = None

    try:
        queue = Queue.Queue()
        start_time = utils.get_formatted_time(constants.SECONDS * 30, TimeFormat.TIME_FORMAT_MS, stream)
        end_time = utils.get_formatted_time(constants.SECONDS * 60, TimeFormat.TIME_FORMAT_MS, stream)
        copy_type = RecordingAttribute.COPY_TYPE_COMMON
        recording = recording_model.Recording(total_recordings=total_recordings, StartTime=start_time, EndTime=end_time,
                                              StreamId=stream, copyType=copy_type)
        for i in range(total_recordings):
            recording_id = recording.get_entry(i).RecordingId
            web_service_objs.append(notification_utils.get_web_service_object(recording_id))
            recording.get_entry(i).UpdateUrl = web_service_objs[i].get_url()

        LOGGER.debug("Recording instance created=%s", recording.serialize())
        response = a8.create_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)

        assert is_valid, error

        recording_id = recording.get_entry(0).RecordingId

        recording_pool = mp_pool.ThreadPool(processes=total_recordings)
        for i in range(total_recordings):
            recording_pool.apply_async(validate_recordings.validate_recording,
                                       (recording.get_entry(i).RecordingId, web_service_objs[i]), callback=queue.put)

        for i in range(total_recordings):
            is_valid, error = queue.get()
            assert is_valid, error

        archive_helper.wait_for_archival(stream, recording_id, Archive.ARCHIVE, Archive.IN_PROGRESS)
        for i in range(total_recordings):
            response = rio.find_recording(recording.get_entry(i).RecordingId).json()
            is_valid, error = validate_storage.validate_recording_in_storage(response, Cos.ARCHIVE_STORAGE,
                                                                             Cos.RECORDING_STORED)

            assert is_valid, error

        for i in range(total_recordings):
            recording_pool.apply_async(validate_recordings.validate_playback,
                                       (recording.get_entry(i).RecordingId,), callback=queue.put)

        for i in range(total_recordings):
            is_valid, error = queue.get()
            assert is_valid, error
    finally:
        if recording_pool:
            recording_pool.close()
            recording_pool.join()
        for web_service_obj in web_service_objs:
            web_service_obj.stop_server()

        response = a8.delete_recording(recording)
        LOGGER.debug("Recording clean up status code=%s", response.status_code)
