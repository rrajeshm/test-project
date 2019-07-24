import Queue
import logging
import multiprocessing.pool as mp_pool
import pytest
import requests
import datetime

import validators.validate_common as validate_common
import validators.validate_storage as validate_storage
import validators.validate_recordings as validate_recordings
import helpers.utils as utils
import helpers.constants as constants
import helpers.vmr.rio.api as rio
import helpers.vmr.a8.a8_helper as a8
import helpers.vmr.archive_helper as archive_helper
import helpers.models.recording as recording_model
import helpers.notification.utils as notification_utils

from helpers.constants import Cos
from helpers.constants import TestLog
from helpers.constants import Archive
from helpers.constants import TimeFormat
from helpers.constants import RecordingAttribute
from helpers.constants import V2pc
from helpers.constants import ValidationError
from helpers.vmr.vmr_helper import delete_vmr_pods

PYTESTMARK = pytest.mark.recording
LOGGER = logging.getLogger(TestLog.TEST_LOGGER)


@utils.test_case_logger
def test_rtc9729_er_005_recording_incomplete_us62460(stream):
    """
    Archiving of INCOMPELTE UNIQUE copy recordings and playback
    """

    web_service_objects = []
    recording_pool = None
    recording = None
    copy_type = RecordingAttribute.COPY_TYPE_UNIQUE
    total_rec = 3
    recording_duration = 30  # in sec

    try:
        queue = Queue.Queue()
        start_time = utils.get_formatted_time(constants.SECONDS * recording_duration, TimeFormat.TIME_FORMAT_MS, stream)
        end_time = utils.get_formatted_time(constants.SECONDS * recording_duration * 5, TimeFormat.TIME_FORMAT_MS,
                                            stream)

        # same start time 10 recordings
        recording = recording_model.Recording(total_recordings=total_rec, StartTime=start_time, EndTime=end_time,
                                              copyType=copy_type, StreamId=stream)

        last_recording_id = recording.Entries[total_rec-1].RecordingId
      
        # get recording id and update url
        for i in range(total_rec):
            recording_id = recording.get_entry(i).RecordingId
            web_service_objects.append(notification_utils.get_web_service_object(recording_id))
            recording.get_entry(i).UpdateUrl = web_service_objects[i].get_url()

        # create recording
        LOGGER.debug("Recording instance created=%s", recording.serialize())
        response = a8.create_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)

        assert is_valid, error
       
        response = rio.find_recording(last_recording_id).json()
        if not response:
            return False, ValidationError.RECORDING_RESPONSE_EMPTY.format(last_recording_id)
        start_time = utils.get_parsed_time(response[0][RecordingAttribute.START_TIME][:-1])

        current_time = datetime.datetime.utcnow()
        wait_time = utils.add_time_to_secs((start_time - current_time), constants.SECONDS)
        if wait_time < 0:
            wait_time = 0

        # validate recording is started
        recording_pool = mp_pool.ThreadPool()  
        for i in range(total_rec):
            recording_pool.apply_async(validate_recordings.validate_notification,
                                       (web_service_objects[i], constants.RecordingStatus.STARTED, wait_time),
                                       callback=queue.put)
        for i in range(total_rec):
            is_valid, error = queue.get()
            assert is_valid, error

        #restarting segment recorder to make INCOMPLETE recording
        is_valid, error = delete_vmr_pods(V2pc.SEGMENT_RECORDER)
        assert is_valid, error

        #Verifying recording INCOMPLETE STATE
        for i in range(total_rec):
            recording_id = recording.get_entry(i).RecordingId
            recording_pool.apply_async(validate_recordings.validate_recording_end_state,
                                       (recording_id, [constants.RecordingStatus.INCOMPLETE]),
                                       dict(web_service_obj=web_service_objects[i],end_time=end_time),
                                       callback=queue.put)

        for i in range(total_rec):
            is_valid, error = queue.get()
            assert is_valid, error

        # recording should have been completed by this time
        # Verifying recording in archive storage
        archive_helper.wait_for_archival(stream, recording.get_entry(0).RecordingId, Archive.ARCHIVE, Archive.COMPLETE)
        for i in range(total_rec):
            response = rio.find_recording(recording.get_entry(i).RecordingId).json()
            is_valid, error = validate_storage.validate_recording_in_storage(response, Cos.ARCHIVE_STORAGE,
                                                                             Cos.RECORDING_STORED,
                                                                             rec_status='INCOMPLETE')
            assert is_valid, error


        # Playback using VLE
        for i in range(total_rec):
            recording_pool.apply_async(validate_recordings.validate_playback_using_vle,
                                       (recording.get_entry(i).RecordingId,), callback=queue.put)

        for i in range(total_rec):
            is_valid, error = queue.get()
            assert is_valid, error


    finally:
        if recording_pool:
            recording_pool.close()
            recording_pool.join()
        for web_service_obj in web_service_objects:
            if web_service_obj:
                web_service_obj.stop_server()

        if recording:
            response = a8.delete_recording(recording)
            LOGGER.debug("Recording clean up status code=%s", response.status_code)
