import logging
import pytest
import requests
import os
import m3u8
import sys
import yaml
import time
import json
import helpers.constants as constants
import helpers.utils as utils
import helpers.models.recording as recording_model
import helpers.notification.utils as notification_utils
import helpers.vmr.a8.a8_helper as a8
import validators.validate_common as validate_common
import validators.validate_recordings as validate_recordings
from helpers.constants import ValidationError
from helpers.constants import RecordingAttribute
from helpers.constants import Component
from helpers.constants import TestLog
from helpers.constants import TimeFormat
from helpers.utils import core_dump
#from helpers.mpe import api as mpe
from helpers import mpe
from helpers.vmr import vmr_helper as vmr
from subprocess import check_output
from helpers.constants import VMR_SERVICES_RESTART
from helpers.constants import V2pc
import helpers.v2pc.v2pc_helper as v2pc

LOGGER = logging.getLogger(os.environ.get("PYTEST_XDIST_WORKER", TestLog.TEST_LOGGER))
CONFIG_INFO = utils.get_configInfo()

V2PC_EXIST = (CONFIG_INFO[Component.GENERIC_CONFIG][Component.ENABLE] == "v2pc")
streams_info = utils.get_source_streams(constants.Stream_tags.MULTIAUDIO)
channels = streams_info[Component.STREAMS]


@utils.test_case_logger
@pytest.mark.parametrize("channel", channels)
@pytest.mark.skipif(not V2PC_EXIST, reason="V2PC Doesn't Exist")
def test_TC10949_validate_pid_based_audio_filtering(channel):
    """
    TC10949: Modify existing publish template to filter audios based
    on pids and verify whether audio filtering is happening or not.
    """
    stream = channel
    web_service_obj = None
    recording = None
    metadata = None
    message = ""
    try:
        audio_pids = utils.get_audio_pids(stream,V2PC_EXIST)
        assert audio_pids, "No manifest response from the given mpd url"
		
        LOGGER.info("audio_pids : %s", audio_pids)
        assert len(audio_pids.keys()) >= 2, "there are no audio pids/only one audio pid available in the selected stream"

        filter_pids = audio_pids.items()[-1]
        LOGGER.info("filter_pids : %s", filter_pids)
        push_payload = {
                  'action': 'disable',
                  'pid': str(filter_pids[0]),
                  'codec': "DD/"+str(filter_pids[1]).upper(),
                  'type': 'audio'
           
                }
        LOGGER.debug("Filtered pids : %s",str(filter_pids[0]))
	
	#Getting default publish template:
        pulished_template = v2pc.get_publish_template(template="HLSTemplate")
        assert pulished_template, "Unable to get the publish template"
        pubished_template_data = json.loads(pulished_template.content)
        LOGGER.debug("Published Template Data : %s", pubished_template_data)
        keystoberemoved = ["externalId", "modified", "sysMeta", "transactionId", "type"]
        metadata = dict([(k, v) for k, v in pubished_template_data.items() if k not in keystoberemoved])
        LOGGER.debug("Modified metadata : %s", metadata)
        metadata_modified = metadata.copy()
        
	#Creating payload with filtered pids:
        stream_config = metadata_modified['properties']['streamConfiguration']
        metadata_modified['properties']['streamConfiguration'] = []
        metadata_modified['properties']['streamConfiguration'].append(push_payload)
        # Add a publish pattern with a name testDP
        LOGGER.debug("Payload to publish template : %s", metadata_modified)
          
        #Updating the publish template with filtered pids payload 
        update_template = v2pc.put_publish_template(metadata_modified, template="HLSTemplate")
        assert update_template, "Unable to update the published template with renamed segment"
	
        # Restart the workflow
        result, response = v2pc.restart_media_workflow(CONFIG_INFO[Component.GENERIC_CONFIG][constants.Component.V2PC]
                                                       [constants.Component.WORK_FLOW][constants.Component.WORKFLOW_NAME])
    
        assert result, response
            
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
        LOGGER.info("validating recording....")
    
        is_valid, error = validate_recordings.validate_recording(recording_id, web_service_obj)
        assert is_valid, error
    
        LOGGER.info("Playback Recording")
        is_valid, error = validate_recordings.validate_playback(recording_id)
        assert is_valid, error
            
        # verifying the m3u8 response
        filtered_pids2 = []
        result = True
        LOGGER.info("verifying m3u8 response..")
        res = m3u8.load(playback_url)
        data = res.data
        LOGGER.debug("m3u8 data after update the publish template : %s", data)
        if type(res.data)==dict:
            if data.has_key('media'):
                for i in data['media']:
                    if i['type']=="AUDIO":
                        filtered_pids2.append(i['name'].split("-")[-1])
                    else:
                        pass
        if len(filtered_pids2)==0 and len(audio_pids) == 2:
            message = "audio pids filtered successfully"
    
        elif len(audio_pids)>2 and len(filtered_pids2)>0:
            if str(filter_pids[0]) not in filtered_pids2:
                message = "audio pids filtered successfully"
            else:
                message = "filtering not happened properly"
                result = False
        assert result, message
        LOGGER.info("Testcase passed with the message : %s",message)       
 
    finally:
        LOGGER.info("Reverting the publish template changes")
        update_template = v2pc.put_publish_template(metadata, template="HLSTemplate")
        assert update_template, "Unable to update the published template with renamed segment"

        # Restart the workflow
        result, response = v2pc.restart_media_workflow(CONFIG_INFO[Component.GENERIC_CONFIG][constants.Component.V2PC]
                                                       [constants.Component.WORK_FLOW][constants.Component.WORKFLOW_NAME])

        assert result, response

        if web_service_obj:
            web_service_obj.stop_server()
        if recording:
            response = a8.delete_recording(recording)
            LOGGER.debug("Recording clean up status code=%s", response.status_code)



@utils.test_case_logger
@pytest.mark.parametrize("channel", channels)
@pytest.mark.skipif(V2PC_EXIST is True, reason="V2PC Exist")
def test_TC10949_validate_pid_based_audio_filtering_no_v2pc(channel):
    """
    TC10949: Modify existing publish template to filter audios based
    on pids and verify whether audio filtering is happening or not.
    """
    stream = channel
    web_service_obj = None
    recording = None
    message = ""
    try:
        service_name = "playout-packager"
        is_valid, config_file = vmr.fetch_config_file(service_name, "mpe-standalone")
        assert is_valid, config_file

        v2pc_config_path = os.path.dirname(config_file)
        config_file_name = os.path.basename(config_file)
        update_path = os.path.join(v2pc_config_path, VMR_SERVICES_RESTART.UPDATE_PATH)
        updated_config = os.path.join(update_path, config_file_name)
        
        audio_pids = utils.get_audio_pids(stream,V2PC_EXIST)
        assert audio_pids, "No manifest response from the given mpd url"
		
        assert len(audio_pids.keys()) >= 2, "there are no audio pids/only one audio pid available in the selected stream"
        
        LOGGER.info("audio_pids : %s", audio_pids)
        filter_pids = audio_pids.items()[-1]
        LOGGER.info("filter_pids : %s", filter_pids)
        push_payload = {
                  'action': 'disable',
                  'pid': str(filter_pids[0]),
                  'codec': str(filter_pids[1]),
                  'type': 'audio'
                }
        
        LOGGER.info("before publish template")
        
        with open(os.path.join(v2pc_config_path, config_file_name), "r") as fp:
            dt = json.load(fp)
            workflows = json.loads(dt['data']['workflows.conf'])
            template = workflows.keys()[0]
            publish_templates = workflows[template]['assetResolver']['workflow'][0]['publishTemplates']
            for templates in publish_templates:
                if templates['name'] == 'HLS':
                    if templates.has_key('streamConfiguration'):
                        templates['streamConfiguration'].append(push_payload)
            workflows[template]['assetResolver']['workflow'][0]['publishTemplates'] = publish_templates
            dt['data']['workflows.conf'] = json.dumps(workflows)
            
        with open(updated_config, 'w') as f:
            json.dump(dt, f, indent=4)
        
        # Apply the config with oc apply -f command
        redeploy_res, resp = vmr.redeploy_config_map(service_name, "mpe-standalone")
        assert redeploy_res, resp
        delete_pods, resp = vmr.delete_vmr_pods("All", "mpe-standalone")
        assert delete_pods, resp
        time.sleep(10)        
        
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
        LOGGER.info("validating recording....")
        is_valid, error = validate_recordings.validate_recording(recording_id, web_service_obj)
        assert is_valid, error

        LOGGER.info("Playback Recording")
        LOGGER.info("recording_id : %s", recording_id)
        is_valid, error = validate_recordings.validate_playback(recording_id)
        assert is_valid, error
        
        # verifying the m3u8 response 
        filtered_pids2 = []
        result = True
        LOGGER.info("verifying m3u8 response..")
        res = m3u8.load(playback_url)
        data = res.data
        LOGGER.debug("m3u8 data after update the publish template : %s", data)
        if type(res.data)==dict:
            if data.has_key('media'):
                for i in data['media']:
                    if i['type']=="AUDIO":
                        filtered_pids2.append(i['name'].split("-")[-1])
                    else:
                        pass
        if len(filtered_pids2)==0 and len(audio_pids) == 2:
            message = "audio pids filtered successfully"
            
        elif len(audio_pids)>2 and len(filtered_pids2)>0:
            if str(filter_pids[0]) not in filtered_pids2:
                message = "audio pids filtered successfully"
            else:
                message = "filtering not happened properly"    
                result = False
        assert result, message
        LOGGER.info("Testcase passed with the message : %s",message)
    finally:
        vmr.redeploy_config_map(service_name, "mpe-standalone", revert=True)
        
        if web_service_obj:
            web_service_obj.stop_server()
        if recording:
            response = a8.delete_recording(recording)
            LOGGER.debug("Recording clean up status code=%s", response.status_code)
