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
import helpers.vmr.rio.api as rio
import validators.validate_recordings as validate_recordings
from helpers.constants import Component
from helpers.constants import DestructiveTesting
from helpers.constants import Interface
from helpers.constants import RecordingAttribute
from helpers.constants import RecordingStatus
from helpers.constants import TestLog
from helpers.constants import TimeFormat
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
@pytest.mark.skip(reason="Defects DE13318 created for this failed Test case")
def test_rtc9801_tc_des_008_mce_ni_block_pending_recording():
    """
    Block traffic on the outgoing MCE interface, trigger a recording(4 minutes) and unblock the interface after 2 minutes
    Check if the recording is INCOMPLETE. Verify the playback of recording
    """
    ssh_client = None
    response = None
    start_duration = 30
    end_duration = 270
    try:
        rev_cmds = {}
        mce_nodes = v2pc.get_app_worker_nodes(MCE_INSTANCE, COMPONENT_NAME)

        for mce_node in mce_nodes:
            mce_data_out = mce_node[Interface.DATA_OUT]
            mce_ip = mce_node[Component.IP]

            ssh_client = utils.get_ssh_client(COMPONENT_NAME, COMPONENT_USERNAME, component_ip=mce_ip)

            # deleting the previously scheduled jobs by other test cases, in order not to tamper with the current test case
            destructive_utils.delete_scheduled_job(COMPONENT_NAME, ssh_client, mce_ip, destructive.MCE_JOB_IDS)

            des_cmd = DestructiveTesting.PACKET_LOSS_OUTGOING_INTERFACE.format(mce_data_out,
                                                                               DestructiveTesting.PACKET_LOSS_BLOCK)
            des_cmd = destructive_utils.get_outgoing_tc_cmd(mce_data_out, des_cmd)

            if mce_node[Interface.DATA_OUT] != mce_node[Interface.MGMT]:
                rev_cmds[mce_ip] = destructive_utils.schedule_rev_cmd(ssh_client, mce_data_out, mce_ip,
                                                                      destructive.MCE_JOB_IDS, constants.MINUTES * 10)

                expected_result = {DestructiveTesting.LOSS: DestructiveTesting.PACKET_LOSS_BLOCK,
                                   DestructiveTesting.DST: DestructiveTesting.NETWORK}
                is_des_effective, error = destructive_utils.exec_des_cmd(ssh_client, mce_data_out, des_cmd, expected_result)
                assert is_des_effective, error
            else:
                destructive_utils.schedule_rev_cmd(ssh_client, mce_data_out, mce_ip, destructive.MCE_JOB_IDS, constants.MINUTES * 2)
                rev_cmds[mce_ip] = None

                LOGGER.info("Executing the command=%s to cause destruction in the component", des_cmd)
                ssh_client.exec_command(des_cmd)

        start_time = utils.get_formatted_time(constants.SECONDS * start_duration, TimeFormat.TIME_FORMAT_MS, STREAM_ID)
        end_time = utils.get_formatted_time(constants.SECONDS * end_duration, TimeFormat.TIME_FORMAT_MS, STREAM_ID)
        response = destructive_utils.create_recording_des(start_time, end_time)

        time.sleep(end_duration + constants.TIME_DELTA)

        for mce_node in mce_nodes:
            mce_ip = mce_node[Component.IP]
            mce_data_out = mce_node[Interface.DATA_OUT]
            ssh_client = utils.get_ssh_client(COMPONENT_NAME, COMPONENT_USERNAME, component_ip=mce_ip)
            if rev_cmds[mce_ip]:
                rev_effective, error = destructive_utils.exec_rev_cmd(COMPONENT_NAME, ssh_client, mce_ip, rev_cmds[mce_ip],
                                                                      mce_data_out, destructive.MCE_JOB_IDS)
            else:
                rev_effective, error = destructive_utils.is_rev_effective(ssh_client, mce_data_out)
            assert rev_effective, error

        is_valid, rec_error = validate_recordings.validate_recording_end_state(
            response[RecordingAttribute.RECORDING_ID], [RecordingStatus.INCOMPLETE],
            web_service_obj=response[RecordingAttribute.WEB_SERVICE_OBJECT])

        recording_response = rio.find_recording(response[RecordingAttribute.RECORDING_ID]).json()
        LOGGER.debug("Recording response=%s", recording_response)

        assert is_valid, rec_error

        # validate playback to check if available segments are recorded
        is_valid, error = validate_recordings.validate_playback(response[RecordingAttribute.RECORDING_ID])

        assert is_valid, error

        # running sanity test to check if the setup is back to normal after reverting the commands
        test_rtc9723_tc_rec_001_future_start_time_future_end_time(STREAM_ID)
    finally:
        if ssh_client:
            ssh_client.close()
        if response:
            response[RecordingAttribute.WEB_SERVICE_OBJECT].stop_server()
            response = a8.delete_recording(response[RecordingAttribute.RECORDING])
            LOGGER.debug("Recording clean up status code=%s", response.status_code)
