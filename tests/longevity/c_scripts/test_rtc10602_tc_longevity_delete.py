import os
import requests
import time
import json
import random
import pytest

import helpers.constants as constants
import helpers.models.recording as recording_model
import helpers.utils as utils
import helpers.vmr.a8.a8_helper as a8
import helpers.vmr.rio.api as rio_api
import validators.validate_common as validate_common
import validators.validate_recordings as validate_recordings
from helpers.models import recording as recording_model
from helpers.constants import ValidationError, TestDirectory


def delete_recordings(del_ct, rec_list, rec_dict):
    """
    Delete the list of Recording from recording list
    :param del_ct: deleting count
    :param rec_list: list of recording idenitifers to delete
    :param rec_dict: recording details of recording
    :return: (rec_list, rec_dict) - recording identifiers list, recording details list
    """

    rec_list_len = len(rec_list)
    print "[INFO: ] rec_list_len ", rec_list_len
    if del_ct <= rec_list_len:
        counts = del_ct
    else:
        counts = rec_list_len

    for inx in range(counts):
        rec_id = rec_list[inx]
        recording = recording_model.Recording(RecordingId=rec_id)
        print "[DEBUG: ] deletion on recid ", rec_id
        response = a8.delete_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)
        assert is_valid, error
        rec_list.remove(rec_id)
        rec_dict['items'].pop(rec_id)
    return rec_list, rec_dict


@utils.test_case_logger
@pytest.mark.run(order=3)
def test_rtc10602_tc_launch_delete(stream):
    """
    TC10602: test module for the longevity delete recordings test suite
    # delete recording as per the configuration for number of deletion and duration for deletion
    """

    longevity_confg = utils.get_spec_config()
    # no need for this numofaccts usage with state 3 dataplane
    # numofaccts = longevity_confg['systemvalues']['numberofaccounts']
    deletespermin = longevity_confg['recording']['recordingspermin']
    test_dur_in_min = longevity_confg['recording']['testdurationinmins']

    try:

        json_file = "recording_list"
        temp_dir = os.path.join(utils.get_base_dir_path(), TestDirectory.TEMP_DIR)
        rec_list_json_file = os.path.join(temp_dir, json_file + '.json')
        assert os.path.exists(rec_list_json_file), ValidationError.FILE_UNAVAILABLE.format(rec_list_json_file)

        with open(rec_list_json_file) as rec_json_file:
            rec_dict = json.load(rec_json_file)
            print rec_dict

        assert rec_dict, ValidationError.FILE_UNAVAILABLE.format(rec_dict)

        # convert yaml parameters to minutes and seconds
        if isinstance(test_dur_in_min, int):
            test_duration_in_seconds = test_dur_in_min * constants.SECONDS_IN_MINUTE

            if isinstance(deletespermin, int):
                # two different scenarios
                # 1: deletion count is lesser than the 60. sleep time will be more than a second.
                # 2: deletion count is more than 60. More than 1 deletion per second.

                deletions_per_sec_rounded = float(deletespermin) / float(test_duration_in_seconds)
                sleep_duration = 1
                if 0.0 < deletions_per_sec_rounded and 1.0 > deletions_per_sec_rounded:
                    deletions_per_sec_rounded = 1
                    sleep_duration = constants.SECONDS_IN_MINUTE / deletespermin
                    total_test_loop_counts = test_duration_in_seconds / sleep_duration
                elif 1 <= int(round(deletions_per_sec_rounded)):
                    deletions_per_sec_rounded = int(round(deletions_per_sec_rounded))
                    sleep_duration = 1
                    total_test_loop_counts = test_duration_in_seconds

                print
                print "[INFO: ] total_test_loop_counts ", total_test_loop_counts
                print "[INFO: ] deletions_per_sec_rounded ", deletions_per_sec_rounded
                print "[INFO: ] sleep duration ", sleep_duration
                print

                rec_list = rec_dict.get('rec_ids')
                for i in range(total_test_loop_counts):
                    rec_list, rec_dict = delete_recordings(deletions_per_sec_rounded, rec_list, rec_dict)
                    time.sleep(sleep_duration)
            else:
                assert False, ValidationError.TYPE_MISMATCH.format('deletespermin is not a number: {0}'.format(deletespermin))

        else:
            assert False, ValidationError.TYPE_MISMATCH.format('testduration is not a number: {0}'.format(testduration))

    finally:
        if rec_list_json_file:
            with open(rec_list_json_file, 'w') as rec_json_file:
                json.dump(rec_dict, rec_json_file)
                print rec_dict
