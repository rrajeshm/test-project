import time
import pytest
import requests
import random
import os
import yaml
import json

from datetime import datetime, timedelta

import helpers.constants as constants
import helpers.models.recording as recording_model
import helpers.notification.utils as notification_utils
import helpers.utils as utils
import helpers.vmr.a8.a8_helper as a8
import helpers.vmr.rio.api as rio_api
import validators.validate_recordings as validate_recordings
import validators.validate_storage as validate_storage

from helpers.constants import Cos, PlaybackTypes, TestDirectory, ValidationError


def launch_vle_playback2(recid):
    response = rio_api.find_recording(recid).json()
    is_valid, error = validate_storage.validate_recording_in_storage(
        response, Cos.ACTIVE_STORAGE, Cos.RECORDING_STORED)
    print "[INFO: ] is_valid active : ", is_valid
    if not is_valid:
        is_valid, error = validate_storage.validate_recording_in_storage(
            response, Cos.ARCHIVE_STORAGE, Cos.RECORDING_STORED)
        # Cos.ARCHIVE_STORAGE
        print "[INFO: ] is_valid archive : ", is_valid
    print "[INFO: ] is_valid : ", is_valid
    if is_valid:
        is_valid, error = validate_recordings.validate_playback(recid)
        print "[INFO: ] playing back recording is completed for recordingid: ", recid
    print "[INFO: ] playback message: ", error


@utils.test_case_logger
@pytest.mark.run(order=2)
def test_rtc10603_tc_longevity_playback(stream):
    """
    TC10603: test module for the longevity playback test suite
    # playback the old available recordings for the given configuration duration and recording counts
    """

    json_file = "recording_list"
    temp_dir = os.path.join(utils.get_base_dir_path(), TestDirectory.TEMP_DIR)
    rec_list_json_file = os.path.join(temp_dir, json_file + '.json')
    assert os.path.exists(rec_list_json_file), ValidationError.FILE_UNAVAILABLE.format(rec_list_json_file)

    with open(rec_list_json_file) as rec_json_file:
        rec_dict = json.load(rec_json_file)
        print rec_dict

    assert rec_dict, ValidationError.FILE_UNAVAILABLE.format(rec_dict)

    longevity_confg = utils.get_spec_config()
    testduration = longevity_confg['playback']['testdurationinmins']
    print '*** show yamal parameters ***'
    print '            testduration:', testduration

    # convert test duration to seconds
    if isinstance(testduration, int):
        test_duration_in_minutes = testduration
        test_duration_in_seconds = testduration * constants.SECONDS_IN_MINUTE

        print '*** run time parameters ***'
        print 'test_duration_in_minutes:', test_duration_in_minutes
        print 'test_duration_in_seconds:', test_duration_in_seconds

        curr_datetime = datetime.now()
        start_datetime = datetime.now()
        end_datetime = curr_datetime + timedelta(minutes=test_duration_in_minutes)
        run_datetime = datetime.now()

        print '*** show test start_time and end_time ***'
        print ' curr_datetime:', curr_datetime
        print 'start_datetime:', start_datetime
        print '  end_datetime:', end_datetime

        rec_list = rec_dict.get('rec_ids')
        while run_datetime < end_datetime:
            print '   .... waiting here until the vle playback is complete'
            print '   .... if vle finishes before end of test duration, run the test again'
            plybk_inx = random.randint(0, (len(rec_list) -1))
            print "[INFO: ] playback index ", plybk_inx
            recid = rec_list[plybk_inx]
            launch_vle_playback2(recid)
            run_datetime = datetime.now()
        print
        print '---- we are done ....'
        print '      test start_time:', start_datetime
        print '        test end_time:', end_datetime
        print ' test actual end_time:', run_datetime
    else:
        assert False, ValidationError.TYPE_MISMATCH.format('testduration is not a number: {0}'.format(testduration))
