import logging
import pytest
import requests
import os
import yaml
import m3u8
import json
import time

import helpers.v2pc.v2pc_helper as v2pc
import helpers.constants as constants
import helpers.models.recording as recording_model
import helpers.notification.utils as notification_utils
import helpers.utils as utils
import helpers.vmr.a8.a8_helper as a8
import validators.validate_common as validate_common
import validators.validate_recordings as validate_recordings

from datetime import datetime
from helpers.constants import RecordingAttribute, ValidationError
from helpers.constants import Component
from helpers.constants import Cos, Feed, V2pc
from helpers.constants import TestLog
from helpers.constants import TimeFormat
from helpers.v2pc import api as v2pc_api

pytestmark = pytest.mark.recording
LOGGER = logging.getLogger(os.environ.get("PYTEST_XDIST_WORKER", TestLog.TEST_LOGGER))
CONFIG_INFO = yaml.load(pytest.config.getoption(constants.GLOBAL_CONFIG_INFO))

COMPONENT_NAME = Component.MCE
COMPONENT_USERNAME = Component.MCE_USERNAME
generic_conf = utils.get_spec_config()


# @pytest.mark.parametrize('stream', streams)
def test_modify_channel_ipdvrtests_57(stream):
    """
    JIRA_URL: https://jira01.engit.synamedia.com/browse/IPDVRTESTS-57
    DESCRIPTION: Modify existing Channels in channel lineup
                 recording the with updated channel and validating playback
    """

    recording = None
    web_service_obj = None
    active_chl_lineup = None
    try:

        generic_confg = utils.get_spec_config()
        workflow = generic_confg[Component.WORK_FLOW][Component.WORKFLOW_NAME]
        response = v2pc_api.get_workflow(workflow)
        workflow_dict = json.loads(response.content)
        LOGGER.debug("[INFO:] workflow_dict %s", workflow_dict)

        assert workflow_dict.get("properties") and workflow_dict["properties"].get("mediaServiceEndpoints"), Feed.CHANNEL_DETAILS_UNAVAILABLE
        for media_service in workflow_dict["properties"]["mediaServiceEndpoints"]:
            if media_service.get("endpointType") == "MEDIASOURCE" and media_service.get("properties") and "assetLineupRef" in media_service["properties"]:
                active_chl_lineup = media_service["properties"]["assetLineupRef"].split(".")[-1]
                break
        assert active_chl_lineup, Feed.CHANNEL_DETAILS_UNAVAILABLE
        LOGGER.debug("[INFO:] active channel lineup detail %s", active_chl_lineup)

        response = v2pc_api.get_channel_line_up(active_chl_lineup)
        LOGGER.debug("[DEBUG:] response %s", response)
        response.raise_for_status()
        LOGGER.debug("[DEBUG:] response content %s", response.content)
        avail_chanl_lineup_dict = json.loads(response.content)

        start_stamp = datetime.utcnow().strftime(TimeFormat.TIME_FORMAT_LOG)
        # Remove the Channel from Lineup
        LOGGER.info("Step 1: Remove existing channel from line up")
        response = v2pc_api.remove_channel_from_channel_line_up(stream, avail_chanl_lineup_dict)
        LOGGER.debug("[DEBUG:] response %s", response)
        assert response, ValidationError.INCORRECT_HTTP_RESPONSE_STATUS_CODE.format(
            response.status_code, response.reason, response.url)
        LOGGER.debug("[DEBUG:] response content %s", response.content)
        original_lineup = json.loads(response.content)

        LOGGER.info("Step 1.1: Validate MCE for Core dump")
        is_valid, msg = utils.core_dump("mce")
        assert is_valid, msg

        LOGGER.info("Step 2: Verify Channel not being captured")
        is_avail = v2pc.is_channel_available(stream)
        assert not is_avail, ValidationError.CHANNEL_EXIST.format(stream)

        # Get the channel details to update
        original_channel_details = v2pc_api.get_channel_source(stream)
        LOGGER.debug("Channel Info : %s", original_channel_details.content)

        LOGGER.info("Step 2.1: Adding channel back to channel line up")
        update_lineup = v2pc_api.add_channel_to_channel_line_up(stream, avail_chanl_lineup_dict)
        assert update_lineup, ValidationError.INCORRECT_HTTP_RESPONSE_STATUS_CODE.format(
            update_lineup.status_code, update_lineup.reason, update_lineup.url)

        LOGGER.info("Step 3: Verify channel is being moving to Capture State")
        LOGGER.info("[INFO:] Waits till stream to move to capture state ")
        is_valid, error = v2pc.waits_till_channel_capture(stream)
        assert is_valid, error

        result, response = v2pc.restart_media_workflow(generic_conf[Component.WORK_FLOW][Component.WORKFLOW_NAME])
        assert result, response
        result, response = v2pc.waits_till_workflow_active(generic_conf[Component.WORK_FLOW][Component.WORKFLOW_NAME])
        assert result, response

        end_stamp = datetime.utcnow().strftime(TimeFormat.TIME_FORMAT_LOG)
        rec_buffer_time = utils.get_rec_duration(dur_confg_key=Component.REC_BUFFER_LEN_IN_SEC)
        rec_duration = utils.get_rec_duration(dur_confg_key=Component.SHORT_REC_LEN_IN_SEC)
        start_time = utils.get_formatted_time((constants.SECONDS * rec_buffer_time), TimeFormat.TIME_FORMAT_MS)
        end_time = utils.get_formatted_time((constants.SECONDS * (rec_buffer_time + rec_duration)), TimeFormat.TIME_FORMAT_MS)
        copy_type = RecordingAttribute.COPY_TYPE_UNIQUE
        LOGGER.debug("Stream Id : %s", stream)
        recording = recording_model.Recording(StartTime=start_time, EndTime=end_time,
                                              copyType=copy_type, StreamId=stream)
        recording_id = recording.get_entry(0).RecordingId
        LOGGER.info("Second Recording Id :%s", recording_id)

        LOGGER.info("STEP 3.1: NO ERROR IN MCE LOGS")
        is_valid, error = v2pc.collect_error_log(
            start_stamp, end_stamp, TestLog.MCE_ERROR_LOG_PATH, recording_id, component=Component.MCE, is_error=True)
        assert is_valid, error

        LOGGER.info("Step 3.2: Validate MCE for Core dump")
        is_valid, msg = utils.core_dump("mce")
        assert is_valid, msg

        # Create a notification handler
        web_service_obj = notification_utils.get_web_service_object(recording_id)
        recording.get_entry(0).UpdateUrl = web_service_obj.get_url()
        LOGGER.debug("Recording instance created : %s", recording.serialize())

        LOGGER.info("Step 4: Create recording on the updated channel and verify its completed")
        response = a8.create_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)
        assert is_valid, error

        is_valid, error = validate_recordings.validate_recording(recording_id, web_service_obj)
        assert is_valid, error

    finally:
        if web_service_obj: web_service_obj.stop_server()
        if recording:
            response = a8.delete_recording(recording)
            LOGGER.debug("Recording clean up status code=%s", response.status_code)

        if active_chl_lineup:
            response = v2pc_api.get_channel_line_up(active_chl_lineup)
            assert response, 'unable to fetch channel line up detail'
            avail_chanl_lineup_dict = json.loads(response.content)
            update_lineup = v2pc_api.add_channel_to_channel_line_up(stream, avail_chanl_lineup_dict)
            assert update_lineup, 'unable to revert the channel line up'
