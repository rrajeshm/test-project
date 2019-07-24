import logging
import pytest
import requests
import os
import yaml
import json
import time

from datetime import datetime
import helpers.constants as constants
import helpers.models.recording as recording_model
import helpers.notification.utils as notification_utils
import helpers.utils as utils
import helpers.vmr.a8.a8_helper as a8
import helpers.vmr.rio.api as rio
import validators.validate_common as validate_common
import validators.validate_recordings as validate_recordings
import validators.validate_storage as validate_storage
from helpers.constants import Cos, Feed, V2pc
from helpers.constants import Component
from helpers.constants import TestLog
from helpers.constants import TimeFormat
from helpers.v2pc import api as v2pc_api
from helpers.v2pc import v2pc_helper
from helpers.vmr.n8 import utils as n8_utils
from helpers.constants import RecordingAttribute, ValidationError

pytestmark = pytest.mark.recording
LOGGER = logging.getLogger(os.environ.get("PYTEST_XDIST_WORKER", TestLog.TEST_LOGGER))
CONFIG_INFO = utils.get_configInfo()

streams_info = utils.get_source_streams(constants.Stream_tags.GENERIC)
# stream is just for reference so picks first stream
channels = streams_info[Component.STREAMS]


V2PC_EXIST = utils.is_v2pc()

COMPONENT_NAME = Component.MCE
COMPONENT_USERNAME = Component.MCE_USERNAME
generic_conf = utils.get_spec_config()


def clean_up_chnl_source(channel_lineup, source_name):
    response = v2pc_api.get_channel_line_up(channel_lineup)
    LOGGER.debug("[DEBUG:] response %s", response)
    # response.raise_for_status()
    LOGGER.debug("[DEBUG:] response content %s", response.content)
    avail_chanl_lineup_dict = json.loads(response.content)

    response = v2pc_api.remove_channel_from_channel_line_up(source_name, avail_chanl_lineup_dict)
    LOGGER.debug("[DEBUG:] response %s", response)
    # response.raise_for_status()
    LOGGER.debug("[DEBUG:] response content %s", response.content)

    delete_resp = v2pc_api.delete_channel_source(source_name)
    if delete_resp:
        channel_details_json = json.loads(delete_resp.content)
        LOGGER.info("[INFO: ] channel_details_json %s", channel_details_json)


# Works with V2PC
# @pytest.mark.parametrize("channel", channels)
@pytest.mark.skipif(V2PC_EXIST is False, reason="V2PC Doesn't Exist")
def test_new_channel_to_lineup_ipdvrtests_56(stream):
    """
    JIRA_URL: https://jira01.engit.synamedia.com/browse/IPDVRTESTS-56
    DESCRIPTION: Adding new channel to lineup with v2pc
                 recording the with new stream and validating playback
    """
    # stream = channel
    recording = None
    web_service_obj = None

    try:
        config_info = utils.get_spec_config()
        workflow = config_info[Component.WORK_FLOW][Component.WORKFLOW_NAME]
        response = v2pc_api.get_workflow(workflow)
        workflow_dict = json.loads(response.content)
        LOGGER.info("[INFO:] workflow_dict %s", workflow_dict)

        active_chl_lineup = None
        assert workflow_dict.get("properties") and workflow_dict["properties"].get(
            "mediaServiceEndpoints"), Feed.CHANNEL_DETAILS_UNAVAILABLE
        for media_service in workflow_dict["properties"]["mediaServiceEndpoints"]:
            if media_service.get("endpointType") == "MEDIASOURCE" and media_service.get(
                    "properties") and "assetLineupRef" in media_service["properties"]:
                active_chl_lineup = media_service["properties"]["assetLineupRef"].split(".")[-1]
                break
        assert active_chl_lineup, Feed.CHANNEL_DETAILS_UNAVAILABLE
        LOGGER.info("[INFO:] active channel lineup detail %s", active_chl_lineup)

        LOGGER.info("[INFO:] config details %s", CONFIG_INFO)
        source_name = V2pc.SAMPLE_CHANNEL_NAME

        clean_up_chnl_source(active_chl_lineup, source_name)
        LOGGER.info("clean up happened... ")

        start_stamp = datetime.utcnow().strftime(TimeFormat.TIME_FORMAT_LOG)
        source_url = config_info[Component.DYNAMIC_CHANNEL_UDP][Component.URL]

        stream_resp = v2pc_api.get_channel_source(stream)
        channel_details_json = json.loads(stream_resp.content)
        LOGGER.info("[INFO:] channel_details_json %s", channel_details_json)

        response = v2pc_api.create_channel_source(source_name, source_url, channel_details_json)
        LOGGER.debug("[DEBUG:] response %s", response)
        response.raise_for_status()
        LOGGER.debug("[DEBUG:] response content %s", response.content)

        response = v2pc_api.get_channel_line_up(active_chl_lineup)
        LOGGER.debug("[DEBUG: ] response %s", response)
        response.raise_for_status()
        LOGGER.debug("[DEBUG:] response content %s", response.content)
        avail_chanl_lineup_dict = json.loads(response.content)

        # Step1: Adding new channel to existing channel lineup
        response = v2pc_api.add_channel_to_channel_line_up(source_name, avail_chanl_lineup_dict)
        LOGGER.debug("[DEBUG:] response %s", response)
        response.raise_for_status()
        LOGGER.debug("[DEBUG: ] response content %s", response.content)

        end_stamp = datetime.utcnow().strftime(TimeFormat.TIME_FORMAT_LOG)

        LOGGER.info("[INFO:] Waits till stream to move to capture state ")
        is_valid, error = v2pc_helper.waits_till_channel_capture(source_name)
        assert is_valid, error


        rec_buffer_time = utils.get_rec_duration(dur_confg_key=Component.REC_BUFFER_LEN_IN_SEC)
        rec_duration = utils.get_rec_duration(dur_confg_key=Component.SHORT_REC_LEN_IN_SEC)

        # default stream is kept default for environment drift calculations
        start_time = utils.get_formatted_time((constants.SECONDS * rec_buffer_time), TimeFormat.TIME_FORMAT_MS, stream)
        end_time = utils.get_formatted_time((constants.SECONDS * (rec_buffer_time + rec_duration)), TimeFormat.TIME_FORMAT_MS, stream)
        recording = recording_model.Recording(StartTime=start_time, EndTime=end_time, StreamId=source_name)
        recording_id = recording.get_entry(0).RecordingId

        result, response = v2pc_helper.restart_media_workflow(generic_conf[Component.WORK_FLOW][Component.WORKFLOW_NAME])
        assert result, response
        
        result, response = v2pc_helper.waits_till_workflow_active(generic_conf[Component.WORK_FLOW][Component.WORKFLOW_NAME])
        assert result, response

        # No errors seen in MCE logs
        is_valid, error = v2pc_helper.collect_error_log(start_stamp, end_stamp, TestLog.MCE_ERROR_LOG_PATH, recording_id, component=Component.MCE, is_error=True)
        assert is_valid, error

        # response = rio.find_recording(recording_id).json()
        LOGGER.info("Core dump validation")
        is_valid, msg = utils.core_dump(Component.MCE)
        assert is_valid, msg

        # Step 2: creating recording on this newly created channel
        web_service_obj = notification_utils.get_web_service_object(recording_id)
        recording.get_entry(0).UpdateUrl = web_service_obj.get_url()
        LOGGER.debug("Recording instance created=%s", recording.serialize())
        response = a8.create_recording(recording)

        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)
        assert is_valid, error

        # Step 3: Verify Recording state is complete
        is_valid, error = validate_recordings.validate_recording(recording_id, web_service_obj)
        assert is_valid, error
        e_stamp = datetime.utcnow().strftime(TimeFormat.TIME_FORMAT_LOG)

        # Step 4: No errors in MA/SR
        is_valid, error  = vmr_helper.verify_error_logs_in_vmr(stream, V2pc.MANIFEST_AGENT, Component.VMR, search_string="ERROR", start_time=start_stamp,end_time=e_stamp)
        assert is_valid, error

        is_valid, error  = vmr_helper.verify_error_logs_in_vmr(stream, V2pc.SEGMENT_RECORDER,Component.VMR, search_string="ERROR", start_time=start_stamp,end_time=e_stamp)
        assert is_valid, error

        # Step 5: Verify Memsql for recording information
        # Alternate validation: Making VMR RIO API call and makes sure recording details available
        response = rio.find_recording(recording_id)
        assert response.content, "Recording details Unavailable"

    finally:
        if web_service_obj: web_service_obj.stop_server()
        if recording:
            response = a8.delete_recording(recording)
            LOGGER.debug("Recording clean up status code=%s", response.status_code)

        if active_chl_lineup and source_name:
            clean_up_chnl_source(active_chl_lineup, source_name)
