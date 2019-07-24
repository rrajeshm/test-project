import logging
import pytest
import requests
import os
import yaml

import helpers.v2pc.v2pc_helper as v2pc
import helpers.constants as constants
import helpers.models.recording as recording_model
import helpers.notification.utils as notification_utils
import helpers.utils as utils
import helpers.vmr.a8.a8_helper as a8
import validators.validate_common as validate_common
import validators.validate_recordings as validate_recordings
from helpers.constants import Component
from helpers.constants import RecordingAttribute, ValidationError
from helpers.constants import TestLog
from helpers.constants import TimeFormat
from helpers.constants import Stream
"""
1. Create a recording
2. During recording in-progress restart the channel 3 times using V2PC API
3. Wait till the recording ends
4. Verify the recorded content. It should be in either Complete state / Incomplete state.
5. Verify the playback
"""

pytestmark = pytest.mark.recording
LOGGER = logging.getLogger(os.environ.get("PYTEST_XDIST_WORKER", TestLog.TEST_LOGGER))
CONFIG_INFO = utils.get_configInfo()

COMPONENT_NAME = Component.MCE
COMPONENT_USERNAME = Component.MCE_USERNAME

streams_info = utils.get_source_streams(constants.Stream_tags.GENERIC)
channels = streams_info[Component.STREAMS]



@utils.test_case_logger
@pytest.mark.parametrize("channel", channels)
def test_tc10367_restart_channel_when_recording(channel):
    """
    JIRA ID : IPDVRTESTS-58 
    JIRA Link : https://jira01.engit.synamedia.com/browse/IPDVRTESTS-58
    TC10367: Restart the channel during recording and then playback
    """
    stream = channel
    recording = None
    web_service_obj = None
    try:
        rec_buffer_time = utils.get_rec_duration(dur_confg_key=Component.REC_BUFFER_LEN_IN_SEC)
        rec_duration = utils.get_rec_duration()
        start_time = utils.get_formatted_time((constants.SECONDS * rec_buffer_time), TimeFormat.TIME_FORMAT_MS, stream)
        end_time = utils.get_formatted_time((constants.SECONDS * (rec_buffer_time + rec_duration)), TimeFormat.TIME_FORMAT_MS, stream)
        copy_type = RecordingAttribute.COPY_TYPE_UNIQUE
        LOGGER.debug("Stream Id : %s", stream)
        recording = recording_model.Recording(StartTime=start_time, EndTime=end_time, copyType=copy_type,
                                              StreamId=stream)
        recording_id = recording.get_entry(0).RecordingId
        LOGGER.info("Recording Id :%s", recording_id)
        web_service_obj = notification_utils.get_web_service_object(recording_id)
        recording.get_entry(0).UpdateUrl = web_service_obj.get_url()
        LOGGER.debug("Recording instance created=%s", recording.serialize())
        response = a8.create_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)
        assert is_valid, error

        LOGGER.info("Restart Channel : %s", stream)

        # Restart the stream
        is_valid, error = v2pc.restart_stream(stream, count=3)
        assert is_valid, error

        is_valid, error = validate_recordings.validate_recording(recording_id, web_service_obj)
        assert is_valid, error


    finally:
        if web_service_obj: web_service_obj.stop_server()
        response = a8.delete_recording(recording)
        LOGGER.debug("Recording clean up status code=%s", response.status_code)
