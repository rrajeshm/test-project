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
def test_rtc9764_tc_rec_028_multiple_delete_playback_common(stream):
    """
    Create multiple recordings with copy type as COMMON, delete one and playback the rest
    """
    total_recordings = 3
    web_service_objs = []
    playback_pool = None
    recording = None

    try:
        queue = Queue.Queue()
        start_time = utils.get_formatted_time(constants.SECONDS * 30, TimeFormat.TIME_FORMAT_MS, stream)
        end_time = utils.get_formatted_time(constants.SECONDS * 60, TimeFormat.TIME_FORMAT_MS, stream)
        copy_type = RecordingAttribute.COPY_TYPE_COMMON
        recording = recording_model.Recording(total_recordings=total_recordings, StartTime=start_time, EndTime=end_time,
                                              copyType=copy_type, StreamId=stream)
        for i in range(total_recordings):
            recording_id = recording.get_entry(i).RecordingId
            web_service_objs.append(notification_utils.get_web_service_object(recording_id))
            recording.get_entry(i).UpdateUrl = web_service_objs[i].get_url()

        LOGGER.debug("Recording instance created=%s", recording.serialize())
        response = a8.create_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)

        assert is_valid, error

        recording_id_0 = recording.get_entry(0).RecordingId

        recording_pool = mp_pool.ThreadPool(processes=total_recordings)
        for i in range(total_recordings):
            recording_pool.apply_async(validate_recordings.validate_recording,
                                       (recording.get_entry(i).RecordingId, web_service_objs[i]), callback=queue.put)

        for i in range(total_recordings):
            is_valid, error = queue.get()
            assert is_valid, error

        for i in range(total_recordings):
            response = rio.find_recording(recording.get_entry(i).RecordingId).json()
            is_valid, error = validate_storage.validate_copy_count(response, Cos.ACTIVE_STORAGE)

            assert is_valid, error

        recording_to_delete = copy.deepcopy(recording)
        del recording_to_delete.get_entries()[1:]
        del recording.get_entries()[:1]  # to clean up recordings later
        response = a8.delete_recording(recording_to_delete)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)

        assert is_valid, error

        is_valid, error = validate_recordings.validate_recording_deletion(recording_id_0)

        assert is_valid, error

        is_valid, error = validate_recordings.validate_playback_using_vle(recording_id_0)

        assert not is_valid, ValidationError.DELETED_RECORDING_PLAYED_BACK.format(recording_id_0)

        playback_pool = mp_pool.ThreadPool(processes=len(recording.get_entries()))
        for recording_entry in recording.get_entries():
            playback_pool.apply_async(validate_recordings.validate_playback_using_vle,
                                      (recording_entry.RecordingId,), callback=queue.put)

        for i in range(len(recording.get_entries())):
            is_valid, error = queue.get()
            assert is_valid, error
    finally:
        if playback_pool:
            playback_pool.close()
            playback_pool.join()
        for web_service_obj in web_service_objs:
            web_service_obj.stop_server()
        if recording:
            response = a8.delete_recording(recording)
            LOGGER.debug("Recording clean up status code=%s", response.status_code)
