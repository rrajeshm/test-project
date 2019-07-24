import Queue
import copy
import logging
import multiprocessing.pool as mp_pool

import pytest
import requests
import os
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

from helpers.constants import Stream
from helpers.constants import Cos
from helpers.constants import TestLog
from helpers.constants import Archive
from helpers.constants import TimeFormat
from helpers.constants import ValidationError
from helpers.constants import RecordingAttribute

pytestmark = pytest.mark.recording
LOGGER = logging.getLogger(os.environ.get("PYTEST_XDIST_WORKER",TestLog.TEST_LOGGER))

TOTAL_RECORDINGS_LIST = [3, 20]

TC_REC_015_033_DATA = [("rtc9761_bulk_recordings_common", RecordingAttribute.COPY_TYPE_COMMON),
                       ("rtc9784_bulk_recordings_unique", RecordingAttribute.COPY_TYPE_UNIQUE)]


@utils.test_case_logger
@pytest.mark.parametrize("total_recording", TOTAL_RECORDINGS_LIST)
@pytest.mark.parametrize("name, copy_type", TC_REC_015_033_DATA, ids=[x[0] for x in
                                                                      TC_REC_015_033_DATA])
def test_tc_rec_033_015_(total_recording, stream, name, copy_type):
    """
    Create multiple recordings with copy type as COMMON
    """

    web_service_objects = []
    recording_pool = None
    recording = None
    try:
        queue = Queue.Queue()
        start_time = utils.get_formatted_time(constants.SECONDS * 30, TimeFormat.TIME_FORMAT_MS, stream)
        end_time = utils.get_formatted_time(constants.SECONDS * 60, TimeFormat.TIME_FORMAT_MS, stream)
        recording = recording_model.Recording(total_recordings=total_recording, StartTime=start_time, EndTime=end_time,
                                              copyType=copy_type, StreamId=stream)
        for i in range(total_recording):
            recording_id = recording.get_entry(i).RecordingId
            web_service_objects.append(notification_utils.get_web_service_object(recording_id))
            recording.get_entry(i).UpdateUrl = web_service_objects[i].get_url()

        LOGGER.debug("Recording instance created=%s", recording.serialize())
        response = a8.create_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)

        assert is_valid, error

        recording_pool = mp_pool.ThreadPool(processes=total_recording)
        for i in range(total_recording):
            recording_pool.apply_async(validate_recordings.validate_recording,
                                       (recording.get_entry(i).RecordingId, web_service_objects[i]), callback=queue.put)

        for i in range(total_recording):
            is_valid, error = queue.get()
            assert is_valid, error

        for i in range(total_recording):
            response = rio.find_recording(recording.get_entry(i).RecordingId).json()
            is_valid, error = validate_storage.validate_recording_in_storage(response, Cos.ACTIVE_STORAGE,
                                                                             Cos.RECORDING_STORED)

            assert is_valid, error

        for i in range(total_recording):
            response = rio.find_recording(recording.get_entry(i).RecordingId).json()
            is_valid, error = validate_storage.validate_recording_in_storage(response, Cos.ACTIVE_STORAGE,
                                                                             Cos.RECORDING_STORED)

            assert is_valid, error

            if copy_type == RecordingAttribute.COPY_TYPE_COMMON:
                is_valid, error = validate_storage.validate_recording_in_storage(response, Cos.ACTIVE_STORAGE,
                                                                                 Cos.RECORDING_NOT_STORED, 1)

                assert is_valid, error

        if copy_type == RecordingAttribute.COPY_TYPE_UNIQUE:
            archive_helper.wait_for_archival(stream, recording.get_entry(0).RecordingId, Archive.ARCHIVE,
                                             Archive.COMPLETE)
            for i in range(total_recording):
                response = rio.find_recording(recording.get_entry(i).RecordingId).json()
                is_valid, error = validate_storage.validate_recording_in_storage(response, Cos.ARCHIVE_STORAGE,
                                                                                 Cos.RECORDING_STORED)
                assert is_valid, error
                is_valid, error = validate_storage.validate_copy_count(response, Cos.ARCHIVE_STORAGE)

                assert is_valid, error

        for i in range(total_recording):
            recording_pool.apply_async(validate_recordings.validate_playback,
                                       (recording.get_entry(i).RecordingId,), callback=queue.put)

        for i in range(total_recording):
            is_valid, error = queue.get()
            assert is_valid, error
    finally:
        if recording_pool:
            recording_pool.close()
            recording_pool.join()
        for web_service_obj in web_service_objects:
            web_service_obj.stop_server()

        response = a8.delete_recording(recording)
        LOGGER.debug("Recording clean up status code=%s", response.status_code)
