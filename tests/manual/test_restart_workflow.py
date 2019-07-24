import logging
import os
import pytest
import requests
import yaml

import helpers.constants as constants
import helpers.models.recording as recording_model
import helpers.notification.utils as notification_utils
import helpers.utils as utils
import helpers.vmr.a8.a8_helper as a8
import validators.validate_common as validate_common
import validators.validate_recordings as validate_recordings
import helpers.v2pc.api as v2pcapi

from helpers.constants import RecordingAttribute
from helpers.constants import Component
from helpers.constants import TestLog
from helpers.constants import TimeFormat
from helpers.constants import MediaWorkflow
from helpers.constants import Stream
from helpers.utils import core_dump


"""
# JIRA ID : IPDVRTESTS-59
# Rally Id : TC10474 
# Description : Disable/Enable Workflow (MFC)
"""
pytestmark = pytest.mark.manifest
LOGGER = logging.getLogger(os.environ.get("PYTEST_XDIST_WORKER", TestLog.TEST_LOGGER))
CONFIG_INFO = yaml.load(pytest.config.getoption(constants.GLOBAL_CONFIG_INFO))

V2PC_EXIST = (CONFIG_INFO[Component.GENERIC_CONFIG][Component.ENABLE] == "v2pc")
streams_info = utils.get_source_streams(constants.Stream_tags.GENERIC)
channels = streams_info[Component.STREAMS]


@pytest.mark.IPDVRTESTS59
@utils.test_case_logger
@pytest.mark.parametrize('channel', channels)
@pytest.mark.skipif(V2PC_EXIST is False, reason="V2PC Doesn't Exist")
def test_ipdvrtests59_restart_workflow(channel):

    recording = None
    web_service_obj = None
    wf_name = None
    try:
        start_time = utils.get_formatted_time((constants.SECONDS * 30), TimeFormat.TIME_FORMAT_MS, channel)
        end_time = utils.get_formatted_time((constants.SECONDS * 60), TimeFormat.TIME_FORMAT_MS, channel)
        copy_type = RecordingAttribute.COPY_TYPE_UNIQUE
        LOGGER.debug("Stream Id : %s", channel)

        recording = recording_model.Recording(StartTime=start_time, EndTime=end_time, copyType=copy_type,
                                              StreamId=channel)

        recording_id = recording.get_entry(0).RecordingId
        LOGGER.info("Recording Id :%s", recording_id)

        # Create a notification handler
        web_service_obj = notification_utils.get_web_service_object(recording_id)
        recording.get_entry(0).UpdateUrl = web_service_obj.get_url()
        LOGGER.debug("Recording instance created :%s", recording.serialize())

        # Create a Recording
        response = a8.create_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)
        assert is_valid, error

        is_valid, error = validate_recordings.validate_recording(recording_id, web_service_obj)
        assert is_valid, error

        # Get the workflow
        wf_name = CONFIG_INFO[Component.GENERIC_CONFIG][constants.Component.V2PC][constants.Component.WORK_FLOW][
            constants.Component.WORKFLOW_NAME]
        LOGGER.info("WorkFlow Name : %s", wf_name)
        resp = v2pcapi.fetch_workflow_status(wf_name)
        LOGGER.info("Media Workflow before disable and enable: %s", resp.content)

        # Verify Channel Capturing status
        channel_res, response = v2pcapi.verify_channel_state(channel, Stream.CAPTURING)
        assert channel_res, response

        # Step 1: Disable a running workflow.
        stop_wf_result, stop_wf_resp = v2pcapi.workflow_change_state(wf_name, MediaWorkflow.DISABLE, time_out=120)
        assert stop_wf_result, stop_wf_resp

        # Verify Channel Capturing status is stopped
        channel_res, response = v2pcapi.verify_channel_state(channel, Stream.CAPTURING)
        assert not channel_res, response

        # Step 2: Enable the workflow again
        start_wf_result, start_wf_resp = v2pcapi.workflow_change_state(wf_name, MediaWorkflow.ENABLE, time_out=120)
        assert start_wf_result, start_wf_resp

        # Check channel goes back to Capturing state
        channel_result, ch_resp = v2pcapi.verify_channel_state(channel, Stream.CAPTURING)
        assert channel_result, "Channel State : %s" % ch_resp

        # Step 3: Validate the playback
        is_valid, error = validate_recordings.validate_playback_using_vle(recording_id)
        assert is_valid, error

        # Check MCE CoreDump
        LOGGER.debug("Validating MCE core dump")
        is_valid, error = core_dump("mce")
        assert is_valid, error

        # Check MPE Logs and CoreDump
        LOGGER.debug("Validating MPE core dump")
        is_valid, error = core_dump("mpe")
        assert is_valid, error

    finally:
        if wf_name:
            v2pcapi.workflow_change_state(wf_name, MediaWorkflow.ENABLE, time_out=120)
        if web_service_obj:
            web_service_obj.stop_server()
        if recording:
            response = a8.delete_recording(recording)
            LOGGER.debug("Recording clean up status code=%s", response.status_code)


@pytest.mark.IPDVRTEST59
@utils.test_case_logger
@pytest.mark.parametrize('channel', channels)
@pytest.mark.skipif(V2PC_EXIST is True, reason="V2PC Exist")
def test_ipdvrtests59_restart_workflow_no_v2pc(channel):
    recording = None
    web_service_obj = None
    mce_wf_name = mpe_wf_name = None
    try:

        start_time = utils.get_formatted_time((constants.SECONDS * 30), TimeFormat.TIME_FORMAT_MS, channel)
        end_time = utils.get_formatted_time((constants.SECONDS * 60), TimeFormat.TIME_FORMAT_MS, channel)
        copy_type = RecordingAttribute.COPY_TYPE_UNIQUE
        LOGGER.debug("Stream Id : %s", channel)

        recording = recording_model.Recording(StartTime=start_time, EndTime=end_time, copyType=copy_type,
                                              StreamId=channel)

        recording_id = recording.get_entry(0).RecordingId
        LOGGER.info("Recording Id :%s", recording_id)

        # Create a notification handler
        web_service_obj = notification_utils.get_web_service_object(recording_id)
        recording.get_entry(0).UpdateUrl = web_service_obj.get_url()
        LOGGER.debug("Recording instance created :%s", recording.serialize())

        # Create a Recording
        response = a8.create_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)
        assert is_valid, error

        is_valid, error = validate_recordings.validate_recording(recording_id, web_service_obj)
        assert is_valid, error

        # Get the Capture only [MCE] workflow
        mce_wf_name = \
            CONFIG_INFO[Component.GENERIC_CONFIG][constants.Component.STANDALONE][constants.Component.WORK_FLOW][
                constants.Component.CAPTURE_ONLY_WORKFLOW]
        LOGGER.info("MCE WorkFlow Name : %s", mce_wf_name)

        # Get the Playback only [MPE] workflow
        mpe_wf_name = \
            CONFIG_INFO[Component.GENERIC_CONFIG][constants.Component.STANDALONE][constants.Component.WORK_FLOW][
                constants.Component.WORKFLOW_NAME]
        LOGGER.info("MPE WorkFlow Name : %s", mpe_wf_name)

        # resp = v2pcapi.fetch_workflow_status(mce_wf_name)
        # LOGGER.info("MCE MediaWorkflow : %s", resp.content)

        # Verify Channel Capturing status
        channel_res, response = v2pcapi.verify_channel_state(channel, Stream.CAPTURING)
        assert channel_res, "Channel is in %s state" % response

        # Step 1: Disable a running Capture only [MCE] workflow.
        stop_mce_result, stop_mce_resp = v2pcapi.workflow_change_state(mce_wf_name, MediaWorkflow.DISABLE, time_out=120)
        assert stop_mce_result, stop_mce_resp

        # Verify Channel Capturing status is stopped
        channel_res, ch_response = v2pcapi.verify_channel_state(channel, Stream.CAPTURING)
        assert not channel_res, ch_response

        # Step 2: Enable the workflow again
        start_mce_rslt, start_mce_resp = v2pcapi.workflow_change_state(mce_wf_name, MediaWorkflow.ENABLE, time_out=120)
        assert start_mce_rslt, start_mce_resp

        # Check channel goes back to Capturing state
        channel_result1, ch_resp1 = v2pcapi.verify_channel_state(channel, Stream.CAPTURING)
        assert channel_result1, "Channel State : %s" % ch_resp1

        # Step 2a: Restart the Playback only workflow
        # resp2 = v2pcapi.fetch_workflow_status(mpe_wf_name)
        # LOGGER.info("MPE MediaWorkflow : %s", resp2.content)

        # Disable a running Playback only [MPE] workflow.
        stop_mpe_wf_result, stop_resp2 = v2pcapi.workflow_change_state(mpe_wf_name, MediaWorkflow.DISABLE, time_out=120)
        assert stop_mpe_wf_result, stop_resp2

        # Enable the MPE workflow again
        start_mpe_wf_result, stop_resp3 = v2pcapi.workflow_change_state(mpe_wf_name, MediaWorkflow.ENABLE, time_out=120)
        assert start_mpe_wf_result, stop_resp3

        # Step 3: Validate the playback
        is_valid, error = validate_recordings.validate_playback_using_vle(recording_id)
        assert is_valid, error

        # Check MCE CoreDump
        LOGGER.debug("Validating MCE core dump")
        is_valid, error = core_dump("mce")
        assert is_valid, error

        # Check MPE Logs and CoreDump
        # It can not be automated - Can not get the core dump for standalone installation of MPE
        # Todo: Need to update this step once the MPE team provides support.

    finally:
        if mce_wf_name:
            v2pcapi.workflow_change_state(mce_wf_name, MediaWorkflow.ENABLE, time_out=120)
        if mpe_wf_name:
            v2pcapi.workflow_change_state(mpe_wf_name, MediaWorkflow.ENABLE, time_out=120)
        if web_service_obj:
            web_service_obj.stop_server()
        if recording:
            response = a8.delete_recording(recording)
            LOGGER.debug("Recording clean up status code=%s", response.status_code)
