import logging
import pytest
import requests
import os
import yaml
import datetime
import json
import time

import validators.validate_storage as validate_storage
import helpers.vmr.rio.api as rio
import helpers.v2pc.v2pc_helper as v2pc
import helpers.constants as constants
import helpers.models.recording as recording_model
import helpers.notification.utils as notification_utils
import helpers.utils as utils
import helpers.vmr.a8.a8_helper as a8
import validators.validate_common as validate_common
import validators.validate_recordings as validate_recordings
import validators.validate_storage as validate_storage
from helpers.vmr import vmr_helper
from helpers.constants import Component
from helpers.constants import RecordingAttribute, ValidationError
from helpers.constants import TestLog
from helpers.constants import TimeFormat
from helpers.constants import Stream
from helpers.constants import Cos

pytestmark = pytest.mark.recording
LOGGER = logging.getLogger(os.environ.get("PYTEST_XDIST_WORKER", TestLog.TEST_LOGGER))
CONFIG_INFO = yaml.load(pytest.config.getoption(constants.GLOBAL_CONFIG_INFO))

COMPONENT_NAME = Component.MCE
COMPONENT_USERNAME = Component.MCE_USERNAME

@utils.test_case_logger
def test_update_recording_ipdvrtests_48(common_lib, stream):
    """
    JIRA_URL    : https://jira01.engit.synamedia.com/browse/IPDVRTESTS-48
    DESCRIPTION : "Update Recording"
    """
    recording = None
    web_service_obj = None
    try:
        #step 1:Create a recording and update the end time. Alternatively update start time if original start time is in future
        ##creating a recording
        recording, web_service_obj = common_lib.create_recording(stream)

        ##change the start time of the recording       
        start_time_new = utils.get_formatted_time(constants.SECONDS * 80, TimeFormat.TIME_FORMAT_MS, stream) 
        recording.get_entry(0).StartTime = start_time_new
        ##change the end time of the recording
        end_time_new = utils.get_formatted_time(constants.SECONDS * 110, TimeFormat.TIME_FORMAT_MS, stream)
        recording.get_entry(0).EndTime = end_time_new
        response = a8.create_recording(recording)
        recording_id = recording.get_entry(0).RecordingId
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)
        assert is_valid, error

        ##verify the recording complete
        is_valid, error = validate_recordings.validate_recording(recording_id, web_service_obj)
        assert is_valid, error 
        
        #step 2:Verify memsql table is updated with new time using RIO API
        response = rio.find_recording(recording_id)
        resp = json.loads(response.content)
        is_valid, error = validate_recordings.validate_time(resp, start_time_new, RecordingAttribute.START_TIME)
        assert is_valid, error
        is_valid, error = validate_recordings.validate_time(resp, end_time_new, RecordingAttribute.END_TIME)
        assert is_valid, error
 
        #step 3:Check cos logs to see segments are written using COS API
        is_valid, error  = validate_storage.validate_recording_in_storage(resp, Cos.ACTIVE_STORAGE, Cos.RECORDING_STORED)
        assert is_valid, error

        #step 4:Check MA/SR pod logs for any errors while recording
        s_time = utils.get_parsed_time(str(start_time_new)[:-1])
        e_time = utils.get_parsed_time(str(end_time_new)[:-1])
        is_valid, error  = vmr_helper.verify_error_logs_in_vmr(stream,'manifest-agent', 'vmr', search_string="ERROR", start_time=s_time,end_time=e_time)
        assert is_valid,error
        is_valid, error  = vmr_helper.verify_error_logs_in_vmr(stream,'segment-recorder', 'vmr', search_string="ERROR", start_time=s_time,end_time=e_time)
        assert is_valid,error

        #step 5:Check no discontinuity errors in MA
        is_valid, error  = vmr_helper.verify_error_logs_in_vmr(stream,'manifest-agent', 'vmr', search_string="discontinuity", start_time=s_time,end_time=e_time)
        assert is_valid,error

    finally:
        if web_service_obj:
            web_service_obj.stop_server()
        if recording:
            response = a8.delete_recording(recording)
            LOGGER.debug("Recording clean up status code=%s", response.status_code)
