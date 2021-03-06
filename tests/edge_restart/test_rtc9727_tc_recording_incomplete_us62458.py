import Queue
import logging
import multiprocessing.pool as mp_pool
import pytest
import requests
import datetime

import validators.validate_common as validate_common
import validators.validate_recordings as validate_recordings

import helpers.utils as utils
import helpers.constants as constants
import helpers.vmr.rio.api as rio
import helpers.vmr.a8.a8_helper as a8
import helpers.models.recording as recording_model
import helpers.notification.utils as notification_utils
from helpers.utils import cleanup
from helpers.vmr.vmr_helper import v2pc_edit_manifest_config
from helpers.vmr.vmr_helper import redeploy_config_map
from helpers.vmr.vmr_helper import verify_batch_size_update
from helpers.vmr.vmr_helper import delete_vmr_pods
from helpers.constants import ValidationError
from helpers.constants import TestLog
from helpers.constants import Component
from helpers.constants import TimeFormat
from helpers.constants import RecordingAttribute
from helpers.constants import V2pc

PYTESTMARK = pytest.mark.recording
LOGGER = logging.getLogger(TestLog.TEST_LOGGER)


TC_ER_003_004_DATA = [("rtc9727_bulk_recordings_common", RecordingAttribute.COPY_TYPE_COMMON)]

@utils.test_case_logger
@pytest.mark.parametrize("name, copy_type", TC_ER_003_004_DATA, ids=[x[0] for x in TC_ER_003_004_DATA])
def test_er_003_004_recording_incomplete(stream, name, copy_type):
    """
    Create multiple recordings with copy type as COMMON
    """

    web_service_objects = []
    recording_pool = None
    recording = None
    total_recording = 20
    diff_start_time_recordings = 10
    same_start_time_recordings = 10
    recording_duration = 30  # in sec
    response_a8 = None
    service_name = V2pc.MANIFEST_AGENT
    namespace = Component.VMR

    try:
        # backup v2pc master config
        is_valid, error = cleanup(redeploy_config_map, service_name, revert=True)
        assert is_valid, error

        is_valid, error = v2pc_edit_manifest_config(V2pc.MANIFEST_AGENT, batch_size='4')
        assert is_valid, error

        is_valid, error = verify_batch_size_update(service_name, namespace, "4")
        assert is_valid, error
        
    
        queue = Queue.Queue()
        start_time = utils.get_formatted_time(constants.SECONDS * recording_duration, TimeFormat.TIME_FORMAT_MS, stream)
        end_time = utils.get_formatted_time(constants.SECONDS * recording_duration * 5, TimeFormat.TIME_FORMAT_MS,
                                            stream)

        # same start time 10 recordings
        recording = recording_model.Recording(total_recordings=same_start_time_recordings, StartTime=start_time,
                                              EndTime=end_time, copyType=copy_type, StreamId=stream)

        # different start time 10 recordings
        for i in range(diff_start_time_recordings, same_start_time_recordings + diff_start_time_recordings):
            start_time = utils.get_formatted_time((constants.SECONDS * recording_duration)+i, TimeFormat.TIME_FORMAT_MS,
                                                  stream)
            end_time = utils.get_formatted_time((constants.SECONDS * recording_duration * 5)+i,
                                                TimeFormat.TIME_FORMAT_MS, stream)
            rec_with_diff_time = recording_model.Recording(total_recordings=diff_start_time_recordings,
                                                           StartTime=start_time, EndTime=end_time, copyType=copy_type,
                                                           StreamId=stream)
            rec_with_diff_time.Entries[0].RecordingId = RecordingAttribute.RECORDING_ID_PREFIX + \
                rec_with_diff_time.RequestId + '_' + str(i)
            recording.Entries.append(rec_with_diff_time.get_entry(0))
        last_recording_id = rec_with_diff_time.Entries[0].RecordingId

        # get recording id and update url
        for i in range(total_recording):
            recording_id = recording.get_entry(i).RecordingId
            web_service_objects.append(notification_utils.get_web_service_object(recording_id))
            recording.get_entry(i).UpdateUrl = web_service_objects[i].get_url()

        # create recording
        LOGGER.debug("Recording instance created=%s", recording.serialize())
        response_a8 = a8.create_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response_a8, requests.codes.no_content)

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
        for i in range(total_recording):
            recording_pool.apply_async(validate_recordings.validate_notification,
                                       (web_service_objects[i], constants.RecordingStatus.STARTED, wait_time),
                                       callback=queue.put)
        for i in range(total_recording):
            is_valid, error = queue.get()
            assert is_valid, error

        #restarting segment recorder to make INCOMPLETE recording
        is_valid, error = delete_vmr_pods(V2pc.SEGMENT_RECORDER)
        assert is_valid, error

        # validate playback using vle
        # verify recording in progress
        for i in range(total_recording):
            recording_pool.apply_async(validate_recordings.validate_playback_using_vle,
                                       (recording.get_entry(i).RecordingId,),
                                       dict(in_progress=True), callback=queue.put)

        for i in range(total_recording):
            is_valid, error = queue.get()
            assert is_valid, error

        # validate playback using hls checker while recording is in progress
        for i in range(total_recording):
            recording_pool.apply_async(validate_recordings.validate_playback_using_hls_checker,
                                       (recording.get_entry(i).RecordingId,), callback=queue.put)

        for i in range(total_recording):
            is_valid, error = queue.get()
            if not is_valid:
                assert 'discontinuity' in error, error
        
        #Verifying recording INCOMPLETE STATE
        for i in range(total_recording):
            recording_id = recording.get_entry(i).RecordingId
            recording_pool.apply_async(validate_recordings.validate_recording_end_state,
                                       (recording_id, [constants.RecordingStatus.INCOMPLETE]),
                                       dict(web_service_obj=web_service_objects[i], end_time=end_time),
                                       callback=queue.put)

        for i in range(total_recording):
            is_valid, error = queue.get()
            assert is_valid, error

        # Playback using VLE
        for i in range(total_recording):
            recording_pool.apply_async(validate_recordings.validate_playback_using_vle,
                                       (recording.get_entry(i).RecordingId,), callback=queue.put)

        for i in range(total_recording):
            is_valid, error = queue.get()
            assert is_valid, error

        # Playback using hls checker
        for i in range(total_recording):
            recording_pool.apply_async(validate_recordings.validate_playback_using_hls_checker,
                                       (recording.get_entry(i).RecordingId,), callback=queue.put)

        for i in range(total_recording):
            is_valid, error = queue.get()
            if not is_valid:
                assert 'discontinuity' in error, error

    finally:
        is_valid, error = cleanup(redeploy_config_map, service_name, revert=True)
        assert is_valid, error

        if recording_pool:
            recording_pool.close()
            recording_pool.join()
        for web_service_obj in web_service_objects:
            if web_service_obj:
                web_service_obj.stop_server()
        
        if response_a8:
            response = a8.delete_recording(recording)
            LOGGER.debug("Recording clean up status code=%s", response.status_code)
