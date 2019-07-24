import pytest
import re
import json
import requests

from helpers.v2pc import api as v2pc_api
from helpers.v2pc import v2pc_helper
from helpers import utils
from validators import validate_common, validate_recordings
from helpers.models import recording as recording_model
from helpers.notification import utils as notification_utils
from helpers.vmr.a8 import a8_helper as a8
from helpers import constants
from helpers.constants import (
    ValidationError, V2pc, Component, PlaybackTypes, Stream_tags,
    TimeFormat, Feed)

LOGGER = utils.get_logger()
V2PC_EXIST = utils.is_v2pc()

streams_info = utils.get_source_streams(Stream_tags.MULTIAUDIO)
channels = streams_info[Component.STREAMS]


@pytest.mark.parametrize("channel", channels)
@pytest.mark.skipif(not V2PC_EXIST, reason="V2PC Doesn't Exist")
def test_dash_wv_pid_based_audio_filtering_ipdvrtests_143(channel):
    """
    JIRA_URL: https://jira01.engit.synamedia.com/browse/IPDVRTESTS-143
    DESCRIPTION: Filtering Audio PID in dash templated and playback with
                 encryption
    """

    package_format = ''
    key_profile_ref = ''
    template_name = None
    pubished_template_data = None
    generic_conf = utils.get_spec_config()
    web_service_obj = None
    recording = None
    metadata = None
    try:
        audio_pids = utils.get_audio_pids(channel, V2PC_EXIST)
        LOGGER.info("Available Audio PIDs : {0}".format(audio_pids))
        assert len(audio_pids.keys()) >= 2, ValidationError.NO_AUDIO_PID
        filter_pids = audio_pids.items()[-1]
        LOGGER.info("filtered pids {0}".format(filter_pids))
        filter_pid_payload = {
            'action': 'disable',
            'pid': str(filter_pids[0]),
            'codec': 'DD/' + str(filter_pids[1]).upper(),
            'type': 'audio'
        }
        LOGGER.info("filtering %s pid from audio pids " % (str(filter_pids[0])))
        LOGGER.info("Audio pids that is available in manifest ")
        templates_list_resp = v2pc_api.get_all_v2pc_templates()
        assert templates_list_resp.status_code == requests.codes.ok, ValidationError.INCORRECT_HTTP_RESPONSE_STATUS_CODE.format(
            templates_list_resp.status_code, templates_list_resp.reason, templates_list_resp.url)
        templt_list = json.loads(templates_list_resp.content)
        for templt in templt_list:
            if templt.get('properties'):
                key_profile_ref = templt['properties'].get('keyProfileRef', '').split('.')[-1]
                package_format = templt['properties'].get('packageFormat', "")
            if (key_profile_ref == V2pc.DASH_TEMPLATE_KEY_PROFILE) and (package_format == V2pc.DASH_TEMPLATE_PACKAGE_FORMAT):
                template_name = templt['name']
                pubished_template_data = templt
                break
        assert key_profile_ref and package_format, ValidationError.DASH_WV_TEMPLATE_UNAVAILABLE

        LOGGER.info("Published Template Data {0}".format(pubished_template_data))
        keys_to_remove = ["externalId", "modified", "sysMeta", "transactionId", "type"]
        metadata = dict([(k, v) for k, v in pubished_template_data.items() if k not in keys_to_remove])
        LOGGER.info("Modified metadata : {0}".format(metadata))
        metadata_modified = metadata.copy()

        stream_config = metadata_modified['properties']['streamConfiguration']
        metadata_modified['properties']['streamConfiguration'] = []
        metadata_modified['properties']['streamConfiguration'].append(filter_pid_payload)

        # Filtering publish templated with PIDs
        LOGGER.info("Payload to publish template : {0}".format(metadata_modified))
        update_template = v2pc_helper.put_publish_template(metadata_modified, template=template_name)
        assert update_template, "Unable to update the published template with renamed segment"

        # Restart the workflow
        result, response = v2pc_helper.restart_media_workflow(generic_conf[Component.WORK_FLOW][Component.WORKFLOW_NAME])
        assert result, response
        result, response = v2pc_helper.waits_till_workflow_active(generic_conf[Component.WORK_FLOW][Component.WORKFLOW_NAME])
        assert result, response

        # Step 1: Create a recording for 30 mins..
        rec_buffer_time = utils.get_rec_duration(dur_confg_key=Component.REC_BUFFER_LEN_IN_SEC)
        rec_duration = utils.get_rec_duration(dur_confg_key=Component.LARGE_REC_LEN_IN_SEC)
        start_time = utils.get_formatted_time((constants.SECONDS * rec_buffer_time), TimeFormat.TIME_FORMAT_MS, channel)
        end_time = utils.get_formatted_time((constants.SECONDS * (rec_buffer_time + rec_duration)), TimeFormat.TIME_FORMAT_MS, channel)
        recording = recording_model.Recording(StartTime=start_time, EndTime=end_time, StreamId=channel)
        recording_id = recording.get_entry(0).RecordingId
        web_service_obj = notification_utils.get_web_service_object(recording_id)
        recording.get_entry(0).UpdateUrl = web_service_obj.get_url()
        LOGGER.debug("Recording instance created=%s", recording.serialize())
        response = a8.create_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)
        assert is_valid, error

        is_valid, error = validate_recordings.validate_recording(recording_id, web_service_obj)
        assert is_valid, error

        # Step 2: Playback using DASH publish template configured to filter based on audio PIDs
        LOGGER.info("Playback Recording with Dash Widevine")
        is_valid, error = validate_recordings.validate_playback(recording_id, playback_types=[PlaybackTypes.DASH_WV, ])
        assert is_valid, error

        filtered_pids2 = []
        result = True

        # Step 3: Verify output of manifest curl to match filtering configured in publish template
        playback_url = utils.get_mpe_playback_url(recording_id, playback_type=PlaybackTypes.DASH_WV)
        resp = requests.get(playback_url)
        xml_val = utils.xml_dict(resp.content)
        LOGGER.info("DASH WV MPD Manifest details : {0}".format(xml_val))

        if xml_val["MPD"]["Period"]:
            for period in xml_val["MPD"]["Period"]:
                for adt_set in period["AdaptationSet"]:
                    if adt_set.has_key('contentType') and adt_set['contentType'] == Feed.AUDIO:
                        for rep in adt_set["Representation"]:
                            LOGGER.info("representation list {0}".format(rep))
                            pids_picked = re.findall(re.compile('audio_\d*'),rep["id"])[-1].replace("audio_", '')
                            if pids_picked:
                                filtered_pids2.append(pids_picked)

        LOGGER.info("filtered_pids2 : {0}".format(filtered_pids2))
        
        if filtered_pids2 and (len(filtered_pids2) < len(audio_pids)) and (str(filter_pids[0]) not in filtered_pids2):
            message = "audio pids filtered successfully"
        else:
            message = "filtering not happened properly"
            result = False

        assert result, message

    finally:
        if web_service_obj:
            web_service_obj.stop_server()
        if recording:
            a8.delete_recording(recording)
            LOGGER.info("recording details destroyed.. ")
        if metadata:
            update_template = v2pc_helper.put_publish_template(metadata, template=template_name)
            assert update_template, "Unable to revert the published template"
            result, response = v2pc_helper.restart_media_workflow(generic_conf[Component.WORK_FLOW][Component.WORKFLOW_NAME])
            assert result, response
