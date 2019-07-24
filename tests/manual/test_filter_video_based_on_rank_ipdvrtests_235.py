import logging
import pytest
import requests
import os
import helpers.v2pc.v2pc_helper as v2pc
import helpers.constants as constants
import helpers.models.recording as recording_model
import helpers.notification.utils as notification_utils
import helpers.utils as utils
import helpers.vmr.a8.a8_helper as a8
from helpers.v2pc import v2pc_helper
import validators.validate_common as validate_common
import validators.validate_recordings as validate_recordings
from helpers.constants import Component
from helpers.constants import TestLog
from helpers.constants import TimeFormat
from helpers.constants import PlaybackTypes
from helpers.constants import V2pc
from helpers.constants import Stream_tags

pytestmark = pytest.mark.recording
LOGGER = logging.getLogger(os.environ.get("PYTEST_XDIST_WORKER", TestLog.TEST_LOGGER))
V2PC_EXIST = utils.is_v2pc()
streams_info = utils.get_source_streams(Stream_tags.SEC_6, Stream_tags.MULTIAUDIO)
channels = streams_info[Component.STREAMS]

@utils.test_case_logger
@pytest.mark.parametrize("channel", channels)
def test_filter_video_based_on_rank_ipdvrtests_235(channel):
    """
    JIRA ID : IPDVRTESTS-235
    JIRA Link : https://jira01.engit.synamedia.com/browse/IPDVRTESTS-235
    Description: DASH-Confirm MPE publish template to filter video based on rank
    """
    stream = channel
    recording = None
    metadata = None
    web_service_obj = None
    try:
        source_xml = utils.get_source_mpd_content(stream)
        bit_res = utils.get_bitrates_resolutions(source_xml)
        bitrates = bit_res.keys()
        assert len(bitrates) >= 2, "Not enough video profiles in the selected stream to filter"

        # Step1: Configure Variants in MPE publish template to filter video based on rank
        push_payload = {
                        "name": "default",
                        "order": "rank",
                        "selectivePublish": "true",
                        "profileOrdering": [
                             {
                                "rank": "1"
                             },
                             {
                                "rank": "2"
                             }
                         ]
                        }
        generic_config = utils.get_spec_config()
        key_profile = generic_config[Component.WORK_FLOW][Component.KEY_PROFILE_DASH_WIDEVINE]
        response, published_template_data, template_name = v2pc_helper.get_publish_template_by_format(template_format=V2pc.DASH_TEMPLATE_PACKAGE_FORMAT, key_profile=key_profile)
        assert response, "Cannot find the template data for the given format/key profile"
        LOGGER.debug("Published Template Data : %s", published_template_data)
        keys_to_be_removed = V2pc.OTHER_KEYS
        metadata = dict([(key, value) for key, value in published_template_data.items() if key not in keys_to_be_removed])
        LOGGER.debug("Modified metadata : %s", metadata)
        metadata_modified = metadata.copy()
        metadata_modified['properties']["variants"] = [push_payload, ]
        LOGGER.debug("modified publish template : %s", metadata_modified)
        update_template = v2pc.put_publish_template(metadata_modified, template=template_name)
        assert update_template, "Unable to update the published template with renamed segment"

        result, response = v2pc.restart_media_workflow(generic_config[Component.WORK_FLOW][Component.WORKFLOW_NAME])
        assert result, response

        result, response = v2pc_helper.waits_till_workflow_active(generic_config[Component.WORK_FLOW][Component.WORKFLOW_NAME], 120)
        assert result, response

        # Step2: Create 30 minute recording
        LOGGER.info("Creating Recording")
        rec_buffer_time = utils.get_rec_duration(dur_confg_key=Component.REC_BUFFER_LEN_IN_SEC)
        rec_duration = utils.get_rec_duration(dur_confg_key=Component.LARGE_REC_LEN_IN_SEC)
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
        playback_url = utils.get_mpe_playback_url(recording_id, PlaybackTypes.DASH_WV)
        is_valid, error = validate_recordings.validate_recording(recording_id, web_service_obj)
        assert is_valid, error


        # step3: Check for manifest for the variants using curl
        mpd_res = requests.get(playback_url)
        res_bitrates = []
        result_dict = utils.xml_dict(mpd_res.content)
        for repre in result_dict['MPD']['Period'][0]['AdaptationSet']:
            if repre.has_key("Representation") and repre.has_key("contentType") and repre["contentType"] == 'video':
                for rate in repre["Representation"]:
                    if "video" in rate['id'] and int(rate['bandwidth']) not in res_bitrates:
                        res_bitrates.append(int(rate['bandwidth']))

        strip_value = len(push_payload["profileOrdering"]) - len(bitrates)
        if strip_value != 0:
            assert sorted(bitrates)[::-1][0:strip_value] == res_bitrates, "Video bitrates are not filtered based on rank given in publish template"
        else:
            assert sorted(bitrates)[::-1] == res_bitrates, "Video bitrates are not filtered based on rank given in publish template"
    finally:
        if metadata:
            LOGGER.info("Reverting the publish template changes")
            update_template = v2pc.put_publish_template(metadata, template=template_name)
            assert update_template, "Unable to update the publish template"

            result, response = v2pc.restart_media_workflow(generic_config[Component.WORK_FLOW][Component.WORKFLOW_NAME])

            assert result, response
            result, response = v2pc_helper.waits_till_workflow_active(generic_config[Component.WORK_FLOW][Component.WORKFLOW_NAME],120)
            assert result, response

        if web_service_obj:
            web_service_obj.stop_server()
        if recording:
            response = a8.delete_recording(recording)
            LOGGER.debug("Recording clean up status code=%s", response.status_code)