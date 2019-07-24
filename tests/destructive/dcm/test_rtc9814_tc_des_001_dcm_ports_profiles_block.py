import logging
import time

import pytest
import yaml

import helpers.constants as constants
import helpers.destructive.utils as destructive_utils
import helpers.utils as utils
import helpers.v2pc.v2pc_helper as v2pc
import helpers.vle.vle_validators_configuration as vle_validators_configuration
import helpers.vmr.a8.a8_helper as a8
import helpers.vmr.nsa.nsa_helper as nsa
import helpers.vmr.rio.api as rio
import validators.validate_recordings as validate_recordings
from helpers.constants import Component
from helpers.constants import DestructiveTesting
from helpers.constants import Interface
from helpers.constants import RecordingAttribute
from helpers.constants import RecordingStatus
from helpers.constants import TestLog
from helpers.constants import TimeFormat
from helpers.constants import ValidationError
from helpers.constants import Vle
from tests import destructive
from tests.sanity.recording import test_rtc9723_tc_rec_001_future_start_time_future_end_time

pytestmark = pytest.mark.destructive

LOGGER = logging.getLogger(TestLog.TEST_LOGGER)

CONFIG_INFO = yaml.load(pytest.config.getoption(constants.GLOBAL_CONFIG_INFO))

COMPONENT_NAME = Component.MCE
COMPONENT_USERNAME = Component.MCE_USERNAME

MCE_INSTANCE = str(CONFIG_INFO[Component.V2PC][Component.WORK_FLOW][Component.CAPTURE_ENDPOINT])

STREAM_1_CONFIG = CONFIG_INFO[Component.V2PC][Component.STREAM_PROFILES][Component.STREAM_1]
STREAM_ID = str(STREAM_1_CONFIG[Component.ID])


@utils.test_case_logger
@pytest.mark.skip(reason="Defect CSCvd04491 created for this failed Test case")
def test_rtc9814_tc_des_001_dcm_ports_profiles_block():
    """
    Block the individual incoming ports of MCE capturing a profile of the video, trigger a recording and
    verify recording is either complete or incomplete, and verify if rest of the profiles can be played back
    """
    ssh_client = None
    response = None

    stream = nsa.get_stream(STREAM_ID)
    if stream:
        profile_data = v2pc.get_stream_profile_data(stream.json()[0][constants.STREAM_NAME])
        profile_port = profile_data[Component.PORT]
        profile_bitrate = int(profile_data[Component.BITRATE])
    else:
        assert False, ValidationError.STREAM_NOT_FOUND.format(STREAM_ID)

    try:
        rev_cmds = {}
        mce_nodes = v2pc.get_app_worker_nodes(MCE_INSTANCE, COMPONENT_NAME)

        for mce_node in mce_nodes:
            mce_data_in = mce_node[Interface.DATA_IN]
            mce_ip = mce_node[Component.IP]

            ssh_client = utils.get_ssh_client(COMPONENT_NAME, COMPONENT_USERNAME, component_ip=mce_ip)

            # deleting the previously scheduled jobs by other test cases, in order not to tamper with the current test case
            destructive_utils.delete_scheduled_job(COMPONENT_NAME, ssh_client, mce_ip, destructive.MCE_JOB_IDS)

            rev_cmds[mce_ip] = destructive_utils.schedule_rev_cmd(ssh_client, mce_data_in, mce_ip,
                                                                  destructive.MCE_JOB_IDS, constants.MINUTES * 10)

            des_cmd = DestructiveTesting.PACKET_LOSS_PORT.format(DestructiveTesting.IFB_INTERFACE,
                                                                 DestructiveTesting.PACKET_LOSS_BLOCK, profile_port)
            des_cmd = destructive_utils.get_incoming_tc_cmd(mce_data_in, des_cmd)

            expected_result = {DestructiveTesting.LOSS: DestructiveTesting.PACKET_LOSS_BLOCK,
                               DestructiveTesting.SRC: DestructiveTesting.NETWORK, DestructiveTesting.DPORT: profile_port}
            is_des_effective, error = destructive_utils.exec_des_cmd(ssh_client, DestructiveTesting.IFB_INTERFACE,
                                                                     des_cmd, expected_result)
            assert is_des_effective, error

        start_time = utils.get_formatted_time(constants.SECONDS * 30, TimeFormat.TIME_FORMAT_MS, STREAM_ID)
        end_time = utils.get_formatted_time(constants.SECONDS * 90, TimeFormat.TIME_FORMAT_MS, STREAM_ID)
        response = destructive_utils.create_recording_des(start_time, end_time)
        recording_id = response[RecordingAttribute.RECORDING_ID]

        recording_status = [RecordingStatus.INCOMPLETE, RecordingStatus.COMPLETE]
        is_valid, error = validate_recordings.validate_recording_end_state(recording_id,
                                                                           recording_status, web_service_obj=response[
                                                                           RecordingAttribute.WEB_SERVICE_OBJECT],
                                                                           end_time=end_time)
        assert is_valid, error

        is_valid, bitrates = utils.get_video_profiles_from_m3u8(recording_id)
        assert is_valid, bitrates
        # If Valid, bitrates will contain the list of video profiles
        assert bitrates, ValidationError.VIDEO_PROFILES_NOT_FOUND.format(recording_id)

        if profile_bitrate not in bitrates:
            assert False, ValidationError.STREAM_BITRATE_UNAVAILABLE_IN_M3U8.format(profile_bitrate, STREAM_ID)
        bitrates.remove(profile_bitrate)

        # verifying if the rest of the profiles can be played back
        playback_error = None
        if bitrates:
            for bitrate in bitrates:
                vle_request_params = {Vle.DOWNLOAD_BITRATE: bitrate}
                is_valid, playback_error = validate_recordings.validate_playback_using_vle(
                    recording_id, VLE_REQUEST_PARAMS=vle_request_params)
                if not is_valid:
                    break
        else:
            is_valid = False
            playback_error = ValidationError.BITARTES_NOT_AVAILABLE_TO_PLAYBACK

        # executing the revert command to undo the destructive commands
        for mce_node in mce_nodes:
            mce_ip = mce_node[Component.IP]
            rev_effective, error = destructive_utils.exec_rev_cmd(COMPONENT_NAME, ssh_client, mce_ip, rev_cmds[mce_ip],
                                                                  DestructiveTesting.IFB_INTERFACE, destructive.MCE_JOB_IDS)
            assert rev_effective, error

        assert is_valid, playback_error

        # running sanity test to check if the setup is back to normal after reverting the commands
        test_rtc9723_tc_rec_001_future_start_time_future_end_time(STREAM_ID)
    finally:
        if ssh_client:
            ssh_client.close()
        if response:
            response[RecordingAttribute.WEB_SERVICE_OBJECT].stop_server()
            response = a8.delete_recording(response[RecordingAttribute.RECORDING])
            LOGGER.debug("Recording clean up status code=%s", response.status_code)
