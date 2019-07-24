import logging
import pytest
import requests
import yaml
import time
import os
import json
import errno

from datetime import datetime

import helpers.constants as constants
import helpers.models.recording as recording_model
import helpers.notification.utils as notification_utils
import helpers.utils as utils
import helpers.vmr.a8.a8_helper as a8
import helpers.vmr.rio.api as rio
import validators.validate_common as validate_common
import validators.validate_recordings as validate_recordings
from helpers.constants import TestLog, RecordingAttribute
from helpers.constants import TimeFormat, TestDirectory
from validators.validate_recordings import validate_recording_avail
import datetime as dt


def send_recording(req_ct, stream, rec_duration, recording_list, rec_idx):
    recording = None
    start_now_start_time_delta = 0
    start_now_end_delta = start_now_start_time_delta + rec_duration
    per_loop_recs = range(req_ct)
    for index in per_loop_recs:
        # start_recording_now
        rand_idx = datetime.utcnow().strftime("%Y%m%d_%H%M")
        start_time = utils.get_formatted_time(constants.SECONDS * start_now_start_time_delta,
                                              TimeFormat.TIME_FORMAT_MS, stream)
        end_time = utils.get_formatted_time(constants.SECONDS * start_now_end_delta, TimeFormat.TIME_FORMAT_MS, stream)
        rec_idx += 1
        recording_id = "longetivity_" + str(rand_idx) + "_" + str(rec_idx)
        recording = recording_model.Recording(StartTime=start_time, EndTime=end_time, StreamId=stream, RecordingId=recording_id)
        web_service_obj = notification_utils.get_web_service_object(recording_id)
        recording.get_entry(0).UpdateUrl = web_service_obj.get_url()
        print 'call a8.create.recording() :',
        print recording_id
        response = a8.create_recording(recording)
        print '.... resp.', response.status_code, response.reason

        is_valid, error = validate_common.validate_http_response_status_code(
            response, requests.codes.no_content)
        # assert is_valid, error

        if web_service_obj:
            web_service_obj.stop_server()

        recording_list['items'].update({recording_id: {"status": True, "msg": error}})
        if is_valid:
            recording_list['rec_ids'].append(recording_id)
        else:
            print "[ERROR: ] ", error
            recording_list['items'].update({recording_id: {"status": False, "msg": error}})
    # print "[INFO: ] recording_list ", recording_list
    return recording_list, rec_idx


@utils.test_case_logger
@pytest.mark.run(order=1)
def test_rtc10604_tc_launch_recordings(stream):
    """
    TC10604: test module for the longevity recordings test suite
    # create recording as per the configuration for number of creation and duration for creation
    """

    rec_idx = 0
    longevity_config_info = utils.get_spec_config()
    testduration = longevity_config_info[RecordingAttribute.RECORDING][Component.TESTDURATION_IN_MINS]
    recordingspermin = longevity_config_info[RecordingAttribute.RECORDING][Component.RECORDINGS_PER_MIN]
    recordingduration = longevity_config_info[RecordingAttribute.RECORDING][Component.LARGE_REC_LEN_IN_SEC]
    print
    print '*** yaml config parameters ***'
    print '    testduration:',testduration
    print 'recordingspermin:',recordingspermin
    print 'recordingduration:', recordingduration

    json_file = "recording_list"
    temp_dir = os.path.join(utils.get_base_dir_path(), TestDirectory.TEMP_DIR)
    assert os.path.isdir(temp_dir), ValidationError.DIR_UNAVAILABLE.format(temp_dir)

    recording_list = {'rec_ids': [], 'items': {}}
    # convert yaml parameters to minutes and seconds
    if isinstance(testduration, int):
        test_duration_in_minutes = testduration
        test_duration_in_seconds = testduration * constants.SECONDS_IN_MINUTE

        print '*** run time parameters ***'
        # print 'test_duration_in_days:', testduration / 24
        print 'test_duration_in_minutes:', test_duration_in_minutes
        print 'test_duration_in_seconds:', test_duration_in_seconds
        if isinstance(recordingspermin, int):
            # two different scenarios
            # 1: recording count is lesser than the 60. sleep time will be more than a second.
            # 2: recording count is more than 60. More than 1 deletion per second.

            # recordings_per_sec_round = float(recordingspermin) / float(constants.SECONDS_IN_MINUTE)
            recordings_per_sec_round = float(recordingspermin) / float(test_duration_in_seconds)
            print "[INFO: ] recordings_per_sec_round ", recordings_per_sec_round
            sleep_duration = 1
            if 0.0 < recordings_per_sec_round and recordings_per_sec_round < 1.0:
                recordings_per_sec_round = 1
                sleep_duration = constants.SECONDS_IN_MINUTE / recordingspermin
                total_test_loop_counts = test_duration_in_seconds / sleep_duration
            elif 1 <= int(round(recordings_per_sec_round)):
                recordings_per_sec_round = int(round(recordings_per_sec_round))
                sleep_duration = 1
                total_test_loop_counts = test_duration_in_seconds

            for idx in range(total_test_loop_counts):
                print "[INFO: ] recordings_per_sec_round: ", recordings_per_sec_round
                print "[INFO: ] sleep duration: ", sleep_duration
                print "[INFO: ] stream: ", stream
                print "[INFO: ] recordingduration: ", recordingduration
                recording_list, rec_idx = send_recording(
                    recordings_per_sec_round, stream, recordingduration, recording_list, rec_idx)
                time.sleep(sleep_duration)
            for rec_id in recording_list.get('rec_ids'):
                status, msg = validate_recording_avail(rec_id)
                recording_list['items'].update({rec_id: {"status": status, "msg": msg}})

            rec_list_json_file = os.path.join(temp_dir, json_file + '.json')
            print "[INFO: ] rec_list_json_file ", rec_list_json_file
            with open(rec_list_json_file, 'w') as rec_list_file_obj:
                json.dump(recording_list, rec_list_file_obj)

            # No assert statements added in creating recordings. Since considering recording ids available.
            last_rec_id = recording_list.get('rec_ids')[-1]
            response = rio.find_recording(last_rec_id).json()
            assert response, ValidationError.RECORDING_RESPONSE_EMPTY.format(last_rec_id)
            sec_to_sleep, msg = utils.get_sec_to_complte_rec(response)

            print "[INFO: ] Waiting till recording moves to complete state in seconds ", sec_to_sleep
            time.sleep(sec_to_sleep)
            print "[INFO: ] wait time completed "
        else:
            assert False, ValidationError.TYPE_MISMATCH.format('recordingspermin is not a number: {0}'.format(recordingspermin))

    else:
        assert False, ValidationError.TYPE_MISMATCH.format('testduration is not a number: {0}'.format(testduration))
