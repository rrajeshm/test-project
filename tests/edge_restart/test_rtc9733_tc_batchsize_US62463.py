import Queue
import logging
import multiprocessing.pool as mp_pool
import pytest
import requests
import datetime

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

from helpers.constants import Component
from helpers.constants import Cos
from helpers.constants import TestLog
from helpers.constants import Archive
from helpers.constants import TimeFormat
from helpers.constants import ValidationError
from helpers.constants import RecordingAttribute
from helpers.constants import V2pc
from helpers.utils import cleanup
from helpers.vmr.vmr_helper import v2pc_edit_manifest_config
from helpers.vmr.vmr_helper import redeploy_config_map
from helpers.vmr.vmr_helper import verify_batch_size_update
from helpers.vmr.vmr_helper import delete_vmr_pods

pytestmark = pytest.mark.recording
LOGGER = logging.getLogger(TestLog.TEST_LOGGER)


TC_ER_008_009_DATA = [("rtc9733_bulk_recordings_unique", RecordingAttribute.COPY_TYPE_UNIQUE)]


@utils.test_case_logger
@pytest.mark.parametrize("name, copy_type", TC_ER_008_009_DATA, ids=[x[0] for x in TC_ER_008_009_DATA])
def test_er_008_009_recording_recovery_manifest_restart_(stream, name, copy_type):
    """
    UNIQUE  and COMMON copy Recording recovery
    """

    web_service_objects = []
    recording_pool = None
    recording = None
    total_recording = 20
    diff_start_time_recordings = 10
    same_start_time_recordings = 10
    service_name = V2pc.MANIFEST_AGENT
    namespace = Component.VMR

    try:
        #Taking backup of v2pc pod config info and editing the config and then restarting the services
        is_valid, error = cleanup(redeploy_config_map, service_name, revert=True)
        assert is_valid, error

        is_valid, error = v2pc_edit_manifest_config(V2pc.MANIFEST_AGENT, batch_size='4')
        assert is_valid, error

        is_valid, error = verify_batch_size_update(service_name, namespace, "4")
        assert is_valid, error

        queue = Queue.Queue()
        start_time = utils.get_formatted_time(constants.SECONDS * 30, TimeFormat.TIME_FORMAT_MS, stream)
        end_time = utils.get_formatted_time(constants.SECONDS * 150, TimeFormat.TIME_FORMAT_MS, stream)
        recording = recording_model.Recording(total_recordings=same_start_time_recordings, StartTime=start_time,
                                              EndTime=end_time, copyType=copy_type, StreamId=stream)

        for i in range(diff_start_time_recordings, same_start_time_recordings + diff_start_time_recordings):
            start_time = utils.get_formatted_time((constants.SECONDS * 30) + i, TimeFormat.TIME_FORMAT_MS, stream)
            end_time = utils.get_formatted_time((constants.SECONDS * 150) + i, TimeFormat.TIME_FORMAT_MS, stream)
            rec_with_diff_time = recording_model.Recording(total_recordings=1, StartTime=start_time,
                                                           EndTime=end_time, copyType=copy_type, StreamId=stream)

            rec_with_diff_time.Entries[0].RecordingId = RecordingAttribute.RECORDING_ID_PREFIX + \
                                                        rec_with_diff_time.RequestId + '_' + str(i)
            recording.Entries.append(rec_with_diff_time.get_entry(0))
        last_recording_id = rec_with_diff_time.Entries[0].RecordingId

        for i in range(total_recording):
            recording_id = recording.get_entry(i).RecordingId
            web_service_objects.append(notification_utils.get_web_service_object(recording_id))
            recording.get_entry(i).UpdateUrl = web_service_objects[i].get_url()

        LOGGER.debug("Recording instance created=%s", recording.serialize())

        #Sending recording request to create recording
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

        #Verifying recording is started or not
        recording_pool = mp_pool.ThreadPool()
        for i in range(total_recording):
            recording_pool.apply_async(validate_recordings.validate_notification,
                                       (web_service_objects[i], constants.RecordingStatus.STARTED, wait_time),
                                       callback=queue.put)

        for i in range(total_recording):
            is_valid, error = queue.get()
            assert is_valid, error


        is_valid, error = delete_vmr_pods(V2pc.MANIFEST_AGENT)
        assert is_valid, error

        response = rio.find_recording(last_recording_id).json()
        if not response:
            return False, ValidationError.RECORDING_RESPONSE_EMPTY.format(last_recording_id)
        end_time = utils.get_parsed_time(response[0][RecordingAttribute.END_TIME][:-1])

        current_time = datetime.datetime.utcnow()
        wait_time = utils.add_time_to_secs((end_time - current_time), constants.SECONDS)
        if wait_time < 0:
            wait_time = 0

        for i in range(total_recording):
            recording_pool.apply_async(validate_recordings.validate_notification,
                                       (web_service_objects[i], constants.RecordingStatus.COMPLETE, wait_time),
                                       callback=queue.put)

        for i in range(total_recording):
            is_valid, error = queue.get()
            assert is_valid, error

        #Verifying recording in storage
        for i in range(total_recording):
            response = rio.find_recording(recording.get_entry(i).RecordingId).json()
            is_valid, error = validate_storage.validate_recording_in_storage(response, Cos.ACTIVE_STORAGE,
                                                                             Cos.RECORDING_STORED)

            assert is_valid, error


        #Verfying Archive storage
        archive_helper.wait_for_archival(stream, recording.get_entry(0).RecordingId, Archive.ARCHIVE, Archive.COMPLETE)
        for i in range(total_recording):
            response = rio.find_recording(recording.get_entry(i).RecordingId).json()
            is_valid, error = validate_storage.validate_recording_in_storage(response, Cos.ARCHIVE_STORAGE,
                                                                             Cos.RECORDING_STORED)
            assert is_valid, error

            is_valid, error = validate_storage.validate_copy_count(response, Cos.ARCHIVE_STORAGE)
            assert is_valid, error

    
    finally:
        is_valid, error = cleanup(redeploy_config_map, service_name, revert=True)
        assert is_valid, error

        if recording_pool:
            recording_pool.close()
            recording_pool.join()
        for web_service_obj in web_service_objects:
            web_service_obj.stop_server()

        if recording:
            response = a8.delete_recording(recording)
            LOGGER.debug("Recording clean up status code=%s", response.status_code)
    


