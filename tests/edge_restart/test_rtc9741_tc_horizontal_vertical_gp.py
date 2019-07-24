import logging
import pytest
import requests

import helpers.utils as utils
import helpers.constants as constants
import helpers.vmr.a8.a8_helper as a8
import helpers.models.recording as recording_model
import helpers.notification.utils as notification_utils
import validators.validate_common as validate_common
import validators.validate_recordings as validate_recordings

from helpers.constants import TestLog
from helpers.constants import TimeFormat
from helpers.constants import RecordingAttribute
from helpers.constants import V2pc
from helpers.v2pc.v2pc_helper import verify_vertical_grouping
from helpers.utils import cleanup
from helpers.vmr.vmr_helper import v2pc_edit_manifest_config
from helpers.vmr.vmr_helper import redeploy_config_map


pytestmark = pytest.mark.recording
LOGGER = logging.getLogger(TestLog.TEST_LOGGER)

TC_ER_017_DATA = [("common", RecordingAttribute.COPY_TYPE_COMMON), ("unique", RecordingAttribute.COPY_TYPE_UNIQUE)]


@utils.test_case_logger
@pytest.mark.parametrize("name, copy_type", TC_ER_017_DATA, ids=[x[0] for x in TC_ER_017_DATA])
def test_rtc9741_tc_er_017_horizontal_vertical_grouping(stream, name, copy_type):
    """
    Horizontal Grouping - Enable horizotal grouping for 12s.
    """

    web_service_obj = None
    recording = None
    grouping_duration = '12s'

    try:
        #Taking backup of v2pc pod config info and editing the config and then restarting the services
        is_valid, error = cleanup(redeploy_config_map, V2pc.MANIFEST_AGENT, revert=True)
        assert is_valid, error

        is_valid,error = v2pc_edit_manifest_config(V2pc.MANIFEST_AGENT, vertical_grouping="*",
                                                   horizontal_grouping=grouping_duration)
        assert is_valid, error

        start_time = utils.get_formatted_time(constants.SECONDS * 30, TimeFormat.TIME_FORMAT_MS, stream)
        end_time = utils.get_formatted_time(constants.SECONDS * 60, TimeFormat.TIME_FORMAT_MS, stream)
        recording = recording_model.Recording(StartTime=start_time, EndTime=end_time, copyType=copy_type,
                                              StreamId=stream)
        recording_id = recording.get_entry(0).RecordingId
        web_service_obj = notification_utils.get_web_service_object(recording_id)
        recording.get_entry(0).UpdateUrl = web_service_obj.get_url()
        LOGGER.debug("Recording instance created=%s", recording.serialize())
        response = a8.create_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)

        assert is_valid, error

        is_valid, error = validate_recordings.validate_recording(recording_id, web_service_obj)

        assert is_valid, error

        is_valid, error = verify_vertical_grouping(recording_id, hg_duration = grouping_duration)
        assert is_valid, error

        is_valid, error = validate_recordings.validate_playback(recording_id)
        assert is_valid, error
        
    finally:
        #Revert back the v2pc config changes
        is_valid, error = cleanup(redeploy_config_map, V2pc.MANIFEST_AGENT, revert=True)
        assert is_valid, error

        if web_service_obj:
            web_service_obj.stop_server()

        if recording:
            response = a8.delete_recording(recording)
            LOGGER.debug("Recording clean up status code=%s", response.status_code)
    


