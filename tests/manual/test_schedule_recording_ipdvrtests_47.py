import logging
import pytest
import requests
import os
import yaml
import json

import helpers.vmr.rio.api as rio
import helpers.constants as constants
import helpers.utils as utils
import helpers.models.recording as recording_model
import helpers.notification.utils as notification_utils
import helpers.vmr.a8.a8_helper as a8
import validators.validate_common as validate_common
import validators.validate_recordings as validate_recordings
import validators.validate_storage as validate_storage

from datetime import datetime
from helpers.vmr import vmr_helper
from helpers.constants import Cos
from helpers.constants import Component
from helpers.constants import TestLog
from helpers.constants import TimeFormat

LOGGER = logging.getLogger(os.environ.get("PYTEST_XDIST_WORKER", TestLog.TEST_LOGGER))
CONFIG_INFO = yaml.load(pytest.config.getoption(constants.GLOBAL_CONFIG_INFO))

#streams_info = utils.get_streams(constants.Stream_tags.GENERIC)
#streams = streams_info[Component.STREAMS]

@utils.test_case_logger
#@pytest.mark.parametrize('stream', streams)
def test_schedule_recording_ipdvrtests_47(stream):
    """
    JIRA_URL : https://jira01.engit.synamedia.com/browse/IPDVRTESTS-47
    DESCRIPTION : Schedule recording(future timing)
    """
    web_service_obj = None
    recording = None
    try:
        # STEP 1: Create recording
        LOGGER.info("Creating Recording")
        rec_buffer_time = utils.get_rec_duration(dur_confg_key=Component.REC_BUFFER_LEN_IN_SEC)
        rec_duration = utils.get_rec_duration(dur_confg_key=Component.SHORT_REC_LEN_IN_SEC)
        start_time = utils.get_formatted_time((constants.SECONDS * rec_buffer_time), TimeFormat.TIME_FORMAT_MS, stream)
        end_time = utils.get_formatted_time((constants.SECONDS * (rec_buffer_time + rec_duration)), TimeFormat.TIME_FORMAT_MS, stream)
        recording = recording_model.Recording(StartTime=start_time, EndTime=end_time, StreamId=stream)
        recording_id = recording.get_entry(0).RecordingId
        web_service_obj = notification_utils.get_web_service_object(recording_id)
        recording.get_entry(0).UpdateUrl = web_service_obj.get_url()
        LOGGER.debug("Recording instance created=%s", recording.serialize())
        response = a8.create_recording(recording)

        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)
        assert is_valid, error

        is_valid, error = validate_recordings.validate_recording(recording_id, web_service_obj)
        response = rio.find_recording(recording_id)
        resp = json.loads(response.content)

        #STEP 2: Check memsql table for recording start time using RIO API
        LOGGER.info("Validate recording start time")
        is_valid, error = validate_start_time(start_time, resp, stream) 
        assert is_valid, error

        #STEP 3: Check cos logs to see segments are written by using COS API
        LOGGER.info("Check segments in cos storage")
        is_valid, error  = validate_storage.validate_recording_in_storage(resp, Cos.ACTIVE_STORAGE, Cos.RECORDING_STORED)
        assert is_valid, error


        #STEP 4: Check MA/SR pod logs for any errors while recording
        LOGGER.info("Check MA/SR pod logs for any errors while recording")
        s_time = utils.get_parsed_time(str(start_time)[:-1])
        e_time = utils.get_parsed_time(str(end_time)[:-1])
        is_valid, error  = vmr_helper.verify_error_logs_in_vmr(stream,'manifest-agent', 'vmr', search_string="ERROR", start_time=s_time,end_time=e_time)
        assert is_valid,error

        is_valid, error  = vmr_helper.verify_error_logs_in_vmr(stream,'segment-recorder', 'vmr', search_string="ERROR", start_time=s_time,end_time=e_time)
        assert is_valid,error

        #STEP 5:  Check no discontinuity errors in MA
        LOGGER.info("Check no discontinuity in MA")
        is_valid, error  = vmr_helper.verify_error_logs_in_vmr(stream,"manifest-agent","vmr", search_string="discontinuity", start_time=s_time,end_time=e_time)
        assert is_valid, error

        #STEP 6: Check recording goes to complete state without any coredumps on MCE/ABRGW
        LOGGER.info("Check core dumps")
        is_valid, msg = utils.core_dump("mce")
        assert is_valid, msg

    finally:
        if web_service_obj:
            web_service_obj.stop_server()
        if recording:
            response = a8.delete_recording(recording)
            LOGGER.debug("Recording clean up status code=%s", response.status_code)



def validate_start_time(start_time, resp, stream):
    starttime = datetime.strptime(start_time[:-1], "%Y-%m-%dT%H:%M:%S.%f")
    actualtime= datetime.strptime(resp[0]["ActualStartTime"][:-1], "%Y-%m-%dT%H:%M:%S.%f")
    config_info = utils.get_configInfo()
    v2pc = (CONFIG_INFO[Component.GENERIC_CONFIG][Component.ENABLE] == "v2pc")
    if v2pc == True:
        mce_ip = config_info[Component.MCE]['v2pc']['node1']['ip']
        mce_workflow = config_info[Component.GENERIC_CONFIG]['v2pc'][Component.WORK_FLOW][Component.WORKFLOW_NAME]
        mpd_url = "http://%s/%s/%s/manifest.mpd"%(mce_ip,mce_workflow,stream)
    else:
        mce_ip = config_info[Component.MCE]['v2pc']['node1']['ip']
        mce_workflow = config_info[Component.MCE]['v2pc']['workflow']
        mpd_url = "http://%s/%s/%s/manifest.mpd"%(mce_ip,mce_workflow,stream)
    manifest = requests.get(mpd_url)
    xml_data = utils.xml_dict( manifest.content)
    seg_duration = int(float(xml_data['MPD']['maxSegmentDuration'][2:-1]))
    if (starttime-actualtime).total_seconds() <= seg_duration:
        return True, ""
    else:
        return False, "Invalid recording information in memsql table"
