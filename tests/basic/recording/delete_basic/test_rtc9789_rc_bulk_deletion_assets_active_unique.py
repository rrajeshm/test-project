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
from helpers.constants import Vle
from helpers.vle import vle_validators_configuration

pytestmark = pytest.mark.recording
LOGGER = logging.getLogger(os.environ.get("PYTEST_XDIST_WORKER",TestLog.TEST_LOGGER))

TC_REC_DATA = [("rtc9789_bulk_delete_active_assets", 20)]

@utils.test_case_logger
@pytest.mark.parametrize("name, total_recording", TC_REC_DATA)

def test_tc_rec_active_unique_asset(stream, name, total_recording):
    """
    Bulk deletion of assets in Active Unique(20 requests)
    """
    recording = None
    web_service_objects = []
    recording_pool = None
    recording_id_list = []
    try:
        queue = Queue.Queue()
        start_time = utils.get_formatted_time(constants.SECONDS * 30, TimeFormat.TIME_FORMAT_MS, stream)
        end_time = utils.get_formatted_time(constants.SECONDS * 60, TimeFormat.TIME_FORMAT_MS, stream)
        copy_type = RecordingAttribute.COPY_TYPE_UNIQUE
        recording = recording_model.Recording(total_recordings=total_recording, StartTime=start_time, EndTime=end_time,
                                              copyType=copy_type, StreamId=stream)

        for i in range(total_recording):
            recording_id = recording.get_entry(i).RecordingId
            recording_id_list.append(recording_id)
            web_service_objects.append(notification_utils.get_web_service_object(recording_id))
            recording.get_entry(i).UpdateUrl = web_service_objects[i].get_url()
        LOGGER.debug("Recording instance created=%s", recording.serialize())
        response = a8.create_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)

        assert is_valid, error
        recording_pool = mp_pool.ThreadPool(processes=total_recording)

        for i in range(total_recording):
            recording_pool.apply_async(validate_recordings.validate_recording,
                                       (recording.get_entry(i).RecordingId, web_service_objects[i]),
                                       callback=queue.put)
        for i in range(total_recording):
            is_valid, error = queue.get()
            assert is_valid, error

        for i in range(total_recording):
            response = rio.find_recording(recording_id_list[i]).json()
            is_valid, error = validate_storage.validate_recording_in_storage(response, Cos.ACTIVE_STORAGE,
                                                                             Cos.RECORDING_STORED)
            assert is_valid, error
        time.sleep(20)
        for i in range(total_recording):
            is_valid, error = validate_recordings.validate_playback(recording.get_entry(i).RecordingId)
            assert is_valid, error

        response = a8.delete_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)
        assert is_valid, error

        for i in range(total_recording):
            is_valid, error = validate_recordings.validate_recording_deletion(recording_id_list[i])
            assert is_valid, error

            is_valid, error = validate_recordings.validate_playback_using_vle(recording_id_list[i])
            assert not is_valid, ValidationError.DELETED_RECORDING_PLAYED_BACK.format(recording_id_list[i])

    finally:
        if recording_pool:
            recording_pool.close()
            recording_pool.join()
        for web_service_obj in web_service_objects:
            web_service_obj.stop_server()
        response = a8.delete_recording(recording)
        LOGGER.debug("Recording clean up status code=%s", response.status_code)

