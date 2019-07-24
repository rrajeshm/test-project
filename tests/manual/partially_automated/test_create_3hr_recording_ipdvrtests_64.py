import logging
import pytest
import os
import yaml
import requests
import time
import m3u8
from pprint import pprint
import helpers.utils as utils
import helpers.constants as constants
import helpers.vmr.a8.a8_helper as a8
import helpers.models.recording as recording_model
import helpers.notification.utils as notification_utils
import validators.validate_common as validate_common
import validators.validate_recordings as validate_recordings
import helpers.vmr.nsa.nsa_helper as nsa
import helpers.v2pc.v2pc_helper as v2pc
import helpers.vmr.rio.api as rio

from helpers.constants import Component
from helpers.constants import TestLog
from helpers.constants import TimeFormat
from helpers.constants import RecordingAttribute
from helpers.constants import V2pc
from helpers.utils import cleanup
from helpers.vmr.vmr_helper import v2pc_edit_manifest_config
from helpers.vmr.vmr_helper import redeploy_config_map
from helpers.v2pc.v2pc_helper import verify_vertical_grouping


LOGGER = logging.getLogger(os.environ.get("PYTEST_XDIST_WORKER", TestLog.TEST_LOGGER))

streams_info = utils.get_source_streams(constants.Stream_tags.ID3)
channels = streams_info[Component.STREAMS]

TC_CC_UC_DATA = [("common", RecordingAttribute.COPY_TYPE_COMMON), ("unique", RecordingAttribute.COPY_TYPE_UNIQUE)]


@utils.test_case_logger
@pytest.mark.parametrize("channel", channels)
@pytest.mark.parametrize("name, copy_type", TC_CC_UC_DATA, ids=[x[0] for x in
                                                                        TC_CC_UC_DATA])
def test_create_3hr_recording_ipdvrtests_64(channel, name, copy_type):
    """
    JIRA_URL : https://jira01.engit.synamedia.com/browse/IPDVRTESTS-64
    DESCRIPTION : Create 3 hr recording
    #Partially automated
        Skipped step(s) : Step 3 - No errors seen in MA/SR pods
    """
    stream = channel
    web_service_obj = None
    recording = None
    stream_name = nsa.get_stream(stream).json()[0][constants.STREAM_NAME]
    try:
        #STEP 1 - Create UC and CC recording with longer duration ~3hr
        start_time = utils.get_formatted_time(constants.SECONDS * 30, TimeFormat.TIME_FORMAT_MS, stream)
        end_time = utils.get_formatted_time(constants.SECONDS * 10830, TimeFormat.TIME_FORMAT_MS, stream)
        recording = recording_model.Recording(StartTime=start_time, EndTime=end_time, copyType=copy_type,
                                              StreamId=stream)
        recording_id = recording.get_entry(0).RecordingId
        web_service_obj = notification_utils.get_web_service_object(recording_id)
        recording.get_entry(0).UpdateUrl = web_service_obj.get_url()
        LOGGER.debug("Recording instance created=%s", recording.serialize())
        response = a8.create_recording(recording)

        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)
        assert is_valid, error

        #STEP 2 - Verify recording state is complete
        is_valid, error = validate_recordings.validate_recording(recording_id, web_service_obj)
        assert is_valid, error

        #Find recording
        LOGGER.info("Find recording in rio")
        response = rio.find_recording(recording_id).json()
        if not response:
            return False, ValidationError.RECORDING_RESPONSE_EMPTY.format(recording_id)
        print "[INFO: ] Recording status in rio : ",response[0]['Status']

        #Playback and Validate Playback
        LOGGER.info("Playback Recording")
        print "\nPlayback Recording"
        print "Recording ID :",recording.get_entry(0).RecordingId
        is_valid, error = validate_recordings.validate_playback(recording.get_entry(0).RecordingId)
        print "[INFO: ] ",is_valid
        assert is_valid, error

        #STEP 4 - memsql table has correct info for UC and CC
        LOGGER.info("Find recording in rio")
        response = rio.find_recording(recording_id).json()
        if not response:
            return False, ValidationError.RECORDING_RESPONSE_EMPTY.format(recording_id)
        print "[INFO: ] Recording status in rio : \n"
        pprint (response, width=1)

    finally:
        if web_service_obj:
            web_service_obj.stop_server()
        if recording:
            response = a8.delete_recording(recording)
            LOGGER.debug("Recording clean up status code=%s", response.status_code)

