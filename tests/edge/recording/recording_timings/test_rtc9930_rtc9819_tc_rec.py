import time
import logging
import datetime
import os
import Queue
import pytest
import requests
import multiprocessing.pool as mp_pool

import helpers.utils as utils
import helpers.constants as constants
import helpers.vmr.rio.api as rio
import helpers.vmr.a8.a8_helper as a8
import helpers.models.recording as recording_model
import helpers.notification.utils as notification_utils
import validators.validate_common as validate_common
import validators.validate_storage as validate_storage
import validators.validate_recordings as validate_recordings

from helpers.constants import Cos
from helpers.constants import TestLog
from helpers.constants import TimeFormat
from helpers.constants import RecordingAttribute

pytestmark = pytest.mark.recording
LOGGER = logging.getLogger(os.environ.get("PYTEST_XDIST_WORKER",TestLog.TEST_LOGGER))

TC_REC_031_032_DATA = [("rtc_9930_030_update_end_time_after_rec_completes_unique", "UNIQUE"),
                       ("rtc9819_031_update_end_time_after_rec_completes_common", "COMMON")]


@utils.test_case_logger
# @pytest.mark.skip(reason = "Defect CSCvc21524 created for this failed Test case")
@pytest.mark.parametrize("name,copy_type", TC_REC_031_032_DATA)
def test_tc_rec(name, stream, copy_type):
    """
    Updating end time of shorter unique copy recording after shorter recording completed,
    and longer recording in progress.
    """

    total_recordings = 2
    validate_rec_wait_time = 240
    recording = None
    web_service_objs = []
    recording_pool = None
    try:
        queue = Queue.Queue()
        start_time = utils.get_formatted_time(constants.SECONDS * 30, TimeFormat.TIME_FORMAT_MS, stream)
        end_time = utils.get_formatted_time(constants.SECONDS * 60, TimeFormat.TIME_FORMAT_MS, stream)
        recording = recording_model.Recording(total_recordings=total_recordings, StartTime=start_time, EndTime=end_time,
                                              copyType=copy_type, StreamId=stream)
        for i in range(total_recordings):
            recording_id = recording.get_entry(i).RecordingId
            web_service_objs.append(notification_utils.get_web_service_object(recording_id))
            recording.get_entry(i).UpdateUrl = web_service_objs[i].get_url()
            LOGGER.debug("Recording=%s, UpdateURL=%s", recording_id, web_service_objs[i].get_url())

        recording.get_entry(1).EndTime = utils.get_formatted_time(constants.SECONDS * 80, TimeFormat.TIME_FORMAT_MS, stream)
        LOGGER.debug("Recording instance created=%s", recording.serialize())
        response = a8.create_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)

        assert is_valid, error

        # Validating longer recording parallel
        recording_id_1 = recording.get_entry(1).RecordingId

        # recording_pool = mp_pool.ThreadPool(processes=2)
        recording_pool = mp_pool.ThreadPool()
        recording_pool.apply_async(validate_recordings.validate_recording, (recording_id_1, web_service_objs[1]),
                                   callback=queue.put)

        # Validating shorter recording to complete
        recording_id_0 = recording.get_entry(0).RecordingId
        is_valid, error = validate_recordings.validate_recording(recording_id_0, web_service_objs[0])

        assert is_valid, error

        shorter_recording_res = rio.find_recording(recording_id_0).json()
        # Validate the copy count for unique/common copy
        if RecordingAttribute.COPY_TYPE_UNIQUE == copy_type:
            is_valid, error = validate_storage.validate_copy_count(shorter_recording_res, Cos.ACTIVE_STORAGE, 2)
        elif RecordingAttribute.COPY_TYPE_COMMON == copy_type:
            is_valid, error = validate_storage.validate_copy_count(shorter_recording_res, Cos.ACTIVE_STORAGE, 1)

        assert is_valid, error
        LOGGER.debug(copy_type + " copy validation success")

        # Try to update the end time after shorter recording completes
        response = rio.find_recording(recording_id_0).json()
        print "[INFO: ] response json ", response

        updated_end_time = utils.get_formatted_time(constants.SECONDS*30, TimeFormat.TIME_FORMAT_MS, stream)
        recording.get_entry(0).EndTime = updated_end_time
        shorter_recording_res = a8.create_recording(recording, recording_id_0)
        response = rio.find_recording(recording_id_0).json()
        print "[INFO: ] response json ", response

        is_valid, error = validate_common.validate_http_response_status_code(shorter_recording_res,
                                                                             requests.codes.bad_request)
        print "[INFO: ] update end time after recording complete message ", error
        # assert is_valid, "CSCvc21524: - "+error

        # Validating the updated end time in response, it should not be equal
        response = rio.find_recording(recording_id_0).json()
        is_valid, error = validate_recordings.validate_time(response, updated_end_time, RecordingAttribute.END_TIME)

        assert not is_valid, error
        print "[INFO: ] Validating the updated end time in response, it should not be equal "
        print "[INFO: ] async validate recording wait time ",time.sleep(validate_rec_wait_time)
        print "[INFO: ] queue list empty ", queue.empty()

        # Validating the longer recording
        is_valid, error = queue.get(timeout=7)
        print "[INFO: ] queue value 1 ", is_valid
        assert is_valid, error
        print "[INFO: ] Validating the longer recording "

        # Playback all recordings
        for i in range(total_recordings):
            recording_pool.apply_async(validate_recordings.validate_playback, (recording.get_entry(i).RecordingId,),
                                       callback=queue.put)
        print "[INFO: ] Playback all recordings "

        # Validate playback recordings
        for i in range(total_recordings):
            is_valid, error = queue.get()
            print "[INFO: ] queue value 2 ", is_valid
            assert is_valid, error
        print "[INFO: ] Validate playback recordings"

    finally:
        if recording_pool:
            recording_pool.close()
            recording_pool.join()
        for web_service_obj in web_service_objs:
            web_service_obj.stop_server()
        response = a8.delete_recording(recording)
        LOGGER.debug("Recording clean up status code=%s", response.status_code)
