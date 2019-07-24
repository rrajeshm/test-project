import Queue
import pytest
import logging
import multiprocessing.pool as mp_pool
import os
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
from helpers.constants import Archive
from helpers.constants import TestLog
from helpers.constants import TimeFormat
from helpers.constants import ValidationError
from helpers.constants import RecordingAttribute

pytestmark = pytest.mark.archival
LOGGER = logging.getLogger(os.environ.get("PYTEST_XDIST_WORKER",TestLog.TEST_LOGGER))

TC_ARC_010_013_DATA = [("rtc9780_010_delete_multiple_unique_copy_recording", False),
                       ("rtc9783_013_delete_multiple_archive_playback_recording", True)]

@utils.test_case_logger
@pytest.mark.parametrize("name, archive_playback", TC_ARC_010_013_DATA,
                         ids=[x[0] for x in TC_ARC_010_013_DATA])
def test_tc_arc(name, stream, archive_playback):
    """
    Schedule recording with same start time, end time with unique copy,
    playback recording single recording and delete it, finally playback remaining records.
    """
    total_recordings = 3
    recording = None
    web_service_objs = []
    recording_pool = None

    try:
        queue = Queue.Queue()
        start_time = utils.get_formatted_time(constants.SECONDS * 30, TimeFormat.TIME_FORMAT_MS, stream)
        end_time = utils.get_formatted_time(constants.SECONDS * 60, TimeFormat.TIME_FORMAT_MS, stream)
        copy_type = RecordingAttribute.COPY_TYPE_UNIQUE
        recording = recording_model.Recording(total_recordings=total_recordings, StartTime=start_time, EndTime=end_time,
                                              copyType=copy_type, StreamId=stream)
        for i in range(total_recordings):
            recording_id = recording.get_entry(i).RecordingId
            web_service_objs.append(notification_utils.get_web_service_object(recording_id))
            recording.get_entry(i).UpdateUrl = web_service_objs[i].get_url()
            LOGGER.debug("Recording=%s, UpdateURL=%s", recording_id, web_service_objs[i].get_url())

        LOGGER.debug("Recording instance created=%s", recording.serialize())
        response = a8.create_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)

        assert is_valid, error

        recording_pool = mp_pool.ThreadPool(processes=total_recordings)
        for i in range(total_recordings):
            recording_pool.apply_async(validate_recordings.validate_recording,
                                       (recording.get_entry(i).RecordingId, web_service_objs[i]), callback=queue.put)

        for i in range(total_recordings):
            is_valid, error = queue.get()
            assert is_valid, error
        # Validating the copy count of unique copy recording in active storage
        for i in range(total_recordings):
            response = rio.find_recording(recording.get_entry(i).RecordingId).json()
            is_valid, error = validate_storage.validate_copy_count(response, Cos.ACTIVE_STORAGE, total_recordings)
            assert is_valid, error

        # Wait till archival completes
        recording_id_0 = recording.get_entry(0).RecordingId
        archive_helper.wait_for_archival(stream, recording_id_0, Archive.ARCHIVE, Archive.COMPLETE)
        # Validating copy count in archive storage after archival duration
        for i in range(total_recordings):
            response = rio.find_recording(recording.get_entry(i).RecordingId).json()
            is_valid, error = validate_storage.validate_recording_in_storage(response, Cos.ACTIVE_STORAGE,
                                                                             Cos.RECORDING_NOT_STORED)

            assert is_valid, error

            is_valid, error = validate_storage.validate_recording_in_storage(response, Cos.ARCHIVE_STORAGE,
                                                                             Cos.RECORDING_STORED)

            assert is_valid, error

            is_valid, error = validate_storage.validate_copy_count(response, Cos.ARCHIVE_STORAGE, 1)
            assert is_valid, error

        if archive_playback:
            response = rio.find_recording(recording_id_0).json()
            # Validating the first recording for playback
            is_valid, error = validate_recordings.validate_playback_using_vle(recording_id_0)
            assert is_valid, error

            # Validating the copy of segments in the archive folder after playback
            is_valid, error = validate_storage.validate_recording_in_storage(response, Cos.ARCHIVE_STORAGE,
                                                                             Cos.RECORDING_STORED)
            assert is_valid, error

            # Validating the segments in recon folder after playback
            is_valid, error = validate_storage.validate_recording_in_storage(response, Cos.RECON_STORAGE,
                                                                             Cos.RECORDING_STORED)
            assert is_valid, error

        response = a8.delete_recording(recording, recording.get_entry(0).RecordingId)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)

        assert is_valid, error

        is_valid, error = validate_recordings.validate_recording_deletion(recording_id_0)

        assert is_valid, error

        is_valid, error = validate_recordings.validate_playback_using_vle(recording_id_0)

        assert not is_valid, ValidationError.DELETED_RECORDING_PLAYED_BACK.format(recording_id_0)

        del recording.get_entries()[:1]
        # Check remaining records still in archive
        if archive_playback:
            for recording_entry in recording.get_entries():
                response = rio.find_recording(recording_entry.RecordingId).json()
                recording_pool.apply_async(validate_storage.validate_recording_in_storage,
                                           (response, Cos.ARCHIVE_STORAGE, Cos.RECORDING_STORED),
                                           callback=queue.put)

            for i in range(len(recording.get_entries())):
                is_valid, error = queue.get()
                assert is_valid, error

        for recording_entry in recording.get_entries():
            recording_pool.apply_async(validate_recordings.validate_playback_using_vle,
                                       (recording_entry.RecordingId,), callback=queue.put)

        for i in range(len(recording.get_entries())):
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
