import logging
import pytest
import requests
import os
import yaml
import json
import time
import m3u8
from pprint import pprint
import helpers.constants as constants
import helpers.utils as utils
import helpers.models.recording as recording_model
import helpers.notification.utils as notification_utils
import helpers.vmr.a8.a8_helper as a8
import helpers.vmr.rio.api as rio
import helpers.vmr.archive_helper as archive_helper
from helpers.vmr import vmr_helper as vmr
import helpers.v2pc.v2pc_helper as v2pc_helper
import validators.validate_common as validate_common
import validators.validate_recordings as validate_recordings
import validators.validate_storage as validate_storage

from helpers.constants import Cos
from helpers.constants import Component
from helpers.constants import TestLog
from helpers.constants import TimeFormat
from helpers.constants import Archive
from helpers.constants import ValidationError
from helpers.constants import RecordingAttribute
from helpers.constants import VMR_SERVICES_RESTART

LOGGER = logging.getLogger(os.environ.get("PYTEST_XDIST_WORKER", TestLog.TEST_LOGGER))
CONFIG_INFO = utils.get_configInfo()

V2PC_EXIST = (CONFIG_INFO[Component.GENERIC_CONFIG][Component.ENABLE] == "v2pc")
streams_info = utils.get_source_streams(constants.Stream_tags.MULTIPLE_AUDIO_CODEC)
channels = streams_info[Component.STREAMS]

gen_config = utils.get_spec_config()
mpe_config = utils.get_spec_config(Component.MPE)

@utils.test_case_logger
@pytest.mark.parametrize("channel", channels)
@pytest.mark.skipif(V2PC_EXIST is False, reason="V2PC Doesn't Exist")
def test_HLS_Verify_codec_based_audio_filtering_V2PC_ipdvrtests_46(channel):
    """
    JIRA ID : IPDVRTESTS-46
    TITLE   : "HLS : Verify codec based audio filtering"
    STEPS   : Create a 30mins recording with future start time. 
              Playback using the HLS publish template configured to filter based on audio codecs.
              Verify output of manifest curl to match the filtering configured in publish template
    """
    stream = channel
    web_service_obj = None
    recording = None
    Actual_publish_template = None
    try:
        audio_codecs = utils.get_audio_codecs(stream,V2PC_EXIST)
        assert audio_codecs, "No manifest response from the given mpd url"
		
        LOGGER.info("audio_codecs : %s", audio_codecs)
        assert len(set(audio_codecs.values())) >= 2, "there is only one audio codec format available in the selected stream"

        # Filtering out one codec from the available default codecs:
        filtered_codec = audio_codecs.items()[-1][1]
        LOGGER.info("filtered_codec : %s", filtered_codec)

        Codec_disabled_payload = []
        for pid, codec in audio_codecs.items():
            if codec == filtered_codec:
                Codec_disabled_payload.append((pid, codec))

        LOGGER.info("Codec_disabled_payload : %s", Codec_disabled_payload)

        #Getting Default Payload from publish template
        Actual_publish_template = v2pc_helper.get_publish_template()

        payload = Actual_publish_template.json()
        LOGGER.info("Default Payload in HLS publish template : %s", payload)

        codec_default_values = [ x['codec'] for x in payload['properties']['streamConfiguration'] ]
        codec_state = [ x['action'] for x in payload['properties']['streamConfiguration'] ]
        LOGGER.info("Default Codec values in HLS publish template : %s", codec_default_values)
        LOGGER.info("Codec States in HLS publish template : %s", codec_state)

        #Creating New payload with filtered codec
        payload['properties']['streamConfiguration'] = []
        for filtered_codec in Codec_disabled_payload:
            payload['properties']['streamConfiguration'].append({
                'action': 'disable',
                'pid': str(filtered_codec[0]),
                'codec': str(filtered_codec[1]).upper(),
                'type': 'audio',
                'default': 'false'})

        LOGGER.info("Updated Payload in HLS publish template (after codec filtering) : %s", payload)
        codec_updated_values = [ x['codec'] for x in payload['properties']['streamConfiguration'] ]
        codec_state = [ x['action'] for x in payload['properties']['streamConfiguration'] ]
        LOGGER.info("Filtered Codec values in HLS publish template : %s", codec_updated_values)
        LOGGER.info("Codec States in HLS publish template : %s", codec_state)

        #Updating HLS Publish template with (Filtered codec) payload
        Updated_publish_template = v2pc_helper.put_publish_template(payload)
        if Updated_publish_template == True:
            LOGGER.info("\nPayload Updated")
        else:
            LOGGER.info("\nPayload Update failed")

        #Restarting the workflow
        LOGGER.info("Restarting the workflow...")
        result, response = v2pc_helper.restart_media_workflow(gen_config[Component.WORK_FLOW][Component.WORKFLOW_NAME])
        assert result, response
        result, response = v2pc_helper.waits_till_workflow_active(gen_config[Component.WORK_FLOW][Component.WORKFLOW_NAME])
        assert result, response

        #Creating Recording with Codec filtering
        LOGGER.info("Creating recording with codec filtering")
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

        LOGGER.info("Validate Recording for %s", recording_id)
        is_valid, error = validate_recordings.validate_recording(recording_id, web_service_obj)
        assert is_valid, error

        #Find recording
        LOGGER.info("Find recording in rio")
        response = rio.find_recording(recording_id).json()
        if not response:
            return False, ValidationError.RECORDING_RESPONSE_EMPTY.format(recording_id)
        LOGGER.info("Recording status in rio : %s", response[0]['Status'])


        #Playback and Validate Playback
        LOGGER.info("Playback Recording of %s", recording.get_entry(0).RecordingId)
        LOGGER.info("Recording ID : %s", recording.get_entry(0).RecordingId)
        is_valid, error = validate_recordings.validate_playback(recording.get_entry(0).RecordingId)
        LOGGER.info("Validate recording : %s", is_valid)
        assert is_valid, error

        #Get Playback URL
        playback_url = utils.get_mpe_playback_url(recording_id)
        LOGGER.info("Playback_ulr ----- %s", playback_url)

        #Validate Filtered Codec Value in m3u8
        filtered_codecs_validation = []
        result = True
        codec_check = m3u8.load(playback_url)
        codec_check_data = codec_check.data
        LOGGER.info("m3u8 playback output:")
        LOGGER.info("codec_check_data : %s", codec_check_data)
        if (type(codec_check_data) == dict) and (codec_check_data.has_key('media')):
                filtered_codecs_validation = [media_data['codecs'] for media_data in codec_check_data['media'] if media_data['type'] == "AUDIO"]

        if filtered_codec not in filtered_codecs_validation:
            message = "Codecs filtered successfully"
        else:
            message = "filtering not happened properly"
            result = False

        assert result, message
        LOGGER.info("Testcase passed with the message : %s",message)

    finally:
        if Actual_publish_template:
            #Reverting Publish template
            LOGGER.info("Reverting default payload ----- %s",Actual_publish_template.json())
            Updated_publish_template = v2pc_helper.put_publish_template(Actual_publish_template.json())
            if Updated_publish_template == True:
                LOGGER.info("Payload reverted")
            else:
                LOGGER.info("Payload reverting failed")

            #Restarting the workflow
            LOGGER.info("Restarting the workflow")
            v2pc_helper.restart_media_workflow(gen_config[Component.WORK_FLOW][Component.WORKFLOW_NAME])
        if web_service_obj:
            web_service_obj.stop_server()
        if recording:
            response = a8.delete_recording(recording)
            LOGGER.debug("Recording clean up status code=%s", response.status_code)


@utils.test_case_logger
@pytest.mark.parametrize("channel", channels)
@pytest.mark.skipif(V2PC_EXIST is True, reason="V2PC Exist")
def test_HLS_Verify_codec_based_audio_filtering_STANDALONE_ipdvrtests_46(channel):
    stream = channel
    web_service_obj = None
    recording = None
    try:
        service_name = "playout-packager"
        is_valid, config_file = vmr.fetch_config_file(service_name, mpe_config[Component.NAMESPACE])
        assert is_valid, config_file
        v2pc_config_path = os.path.dirname(config_file)
        config_file_name = os.path.basename(config_file)
        update_path = os.path.join(v2pc_config_path, VMR_SERVICES_RESTART.UPDATE_PATH)
        updated_config = os.path.join(update_path, config_file_name)

        audio_codecs = utils.get_audio_codecs(stream,V2PC_EXIST)
        assert audio_codecs, "No manifest response from the given mpd url"
		
        LOGGER.info("Audio Codecs available in selected Stream are : %s", audio_codecs)
        assert len(set(audio_codecs.values())) >= 2, "there is only one audio codec format available in the selected stream"

        # Filtering out one codec from the available default codecs:
        filtered_codec = audio_codecs.items()[0][1]
        LOGGER.info("filter_codecs : %s", filtered_codec)

        Codec_disabled_payload = []
        for pid, codec in audio_codecs.items():
            if codec == filtered_codec:
                Codec_disabled_payload.append((pid, codec))

        LOGGER.info("Codec_disabled_payload : %s", Codec_disabled_payload)

        push_payload = []
        for filtered_codec in Codec_disabled_payload:
            push_payload.append({
                        'action': 'disable',
                        'pid': str(filtered_codec[0]),
                        'codec': str(filtered_codec[1]).upper(),
                        'type': 'audio',
                        'default': 'false'
                        })
     
        with open(os.path.join(v2pc_config_path, config_file_name), "r") as fp:
            dt = json.load(fp)
            workflows = json.loads(dt['data']['workflows.conf'])
            template = workflows.keys()[0]
            publish_templates = workflows[template]['assetResolver']['workflow'][0]['publishTemplates']
            for templates in publish_templates:
                if templates['name'] == 'HLS':
                    if templates.has_key('streamConfiguration'):
                        LOGGER.info("Default Payload in HLS publish template : %s", templates['streamConfiguration'])
                        #Updating Payload
                        templates['streamConfiguration'] = []
                        templates['streamConfiguration'].extend(push_payload)
                        LOGGER.info("Updated Payload after codec filtering : %s", templates['streamConfiguration']) 
            workflows[template]['assetResolver']['workflow'][0]['publishTemplates'] = publish_templates
            dt['data']['workflows.conf'] = json.dumps(workflows)
        with open(updated_config, 'w') as f:
            json.dump(dt, f, indent=4)

        # Apply the config with oc apply -f command
        redeploy_res, resp = vmr.redeploy_config_map(service_name, mpe_config[Component.NAMESPACE])
        assert redeploy_res, resp
        delete_pods, resp = vmr.delete_vmr_pods("All", mpe_config[Component.NAMESPACE])
        assert delete_pods, resp

        # Create recording
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
        playback_url = utils.get_mpe_playback_url(recording_id, "hls")
        LOGGER.info("playback_url : %s", playback_url)
        is_valid, error = validate_recordings.validate_recording(recording_id, web_service_obj)
        assert is_valid, error

        #Find recording
        LOGGER.info("Find recording in rio")
        response = rio.find_recording(recording_id).json()
        if not response:
            return False, ValidationError.RECORDING_RESPONSE_EMPTY.format(recording_id)
        LOGGER.info("Recording status in rio : %s", response[0]['Status'])

        #Playback and Validate Playback
        LOGGER.info("Playback Recording for the recording ID : %s", recording.get_entry(0).RecordingId)
        is_valid, error = validate_recordings.validate_playback(recording.get_entry(0).RecordingId)
        LOGGER.info("Validate recording : %s", is_valid)
        assert is_valid, error

        #Get Playback URL
        playback_url = utils.get_mpe_playback_url(recording_id)
        LOGGER.info("Playback_ulr ----- %s", playback_url)

        #Validate Filtered Codec Value in m3u8
        filtered_codecs_validation = []
        result = True
        codec_check = m3u8.load(playback_url)
        codec_check_data = codec_check.data
        LOGGER.info("m3u8 playback output :")
        LOGGER.info("codec_check_data : %s", codec_check_data)
        if (type(codec_check_data) == dict) and (codec_check_data.has_key('media')):
                filtered_codecs_validation = [media_data['codecs'] for media_data in codec_check_data['media'] if media_data['type'] == "AUDIO"]

        if filter_codecs not in filtered_codecs_validation:
            message = "Codecs filtered successfully"
        else:
            message = "filtering not happened properly"
            result = False

        assert result, message
        LOGGER.info("Testcase passed with the message : %s",message)

    finally:
        LOGGER.info("Reverting default payload...")
        vmr.redeploy_config_map(service_name, mpe_config[Component.NAMESPACE], revert=True)

        if web_service_obj:
            web_service_obj.stop_server()
        if recording:
            response = a8.delete_recording(recording)
            LOGGER.debug("Recording clean up status code=%s", response.status_code)
                                                                                          
