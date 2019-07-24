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
@pytest.mark.skip(reason="Defect CSCvd04491, CSCvd85034 created for this failed Test case")
def test_rtc9813_tc_des_006_dcm_multi_cast_ip_packet_loss_recording():
    """
    Introduce 30% packet loss on the Incoming MCE network interface throughout the recording life time,
    Validate recording state against INCOMPLETE or COMPLETE and number of available segments recorded.
    """
    ssh_client = None
    response = None
    web_service_obj = None
    try:
        rev_cmds = {}
        mce_nodes = v2pc.get_app_worker_nodes(MCE_INSTANCE, COMPONENT_NAME)

        for mce_node in mce_nodes:
            mce_data_in = mce_node[Interface.DATA_IN]
            mce_ip = mce_node[Component.IP]

            ssh_client = utils.get_ssh_client(COMPONENT_NAME, COMPONENT_USERNAME, component_ip=mce_ip)

            # deleting the previously scheduled jobs by other test cases, in order not to tamper with the current test case
            destructive_utils.delete_scheduled_job(COMPONENT_NAME, ssh_client, mce_ip, destructive.MCE_JOB_IDS)

            if mce_node[Interface.DATA_IN] != mce_node[Interface.MGMT]:
                rev_cmds[mce_ip] = destructive_utils.schedule_rev_cmd(ssh_client, mce_data_in, mce_ip,
                                                                      destructive.MCE_JOB_IDS, constants.MINUTES * 10)
            else:
                destructive_utils.schedule_rev_cmd(ssh_client, mce_data_in, mce_ip, destructive.MCE_JOB_IDS, constants.MINUTES * 2)
                rev_cmds[mce_ip] = None

            des_cmd = DestructiveTesting.PACKET_LOSS_INCOMING_INTERFACE.format(DestructiveTesting.IFB_INTERFACE,
                                                                               DestructiveTesting.PACKET_LOSS)
            des_cmd = destructive_utils.get_incoming_tc_cmd(mce_data_in, des_cmd)

            # expected outcome after the destructive commands are run
            expected_result = {DestructiveTesting.LOSS: DestructiveTesting.PACKET_LOSS,
                               DestructiveTesting.SRC: DestructiveTesting.NETWORK}
            is_des_effective, error = destructive_utils.exec_des_cmd(ssh_client, DestructiveTesting.IFB_INTERFACE,
                                                                     des_cmd, expected_result)
            assert is_des_effective, error

        start_time = utils.get_formatted_time(constants.SECONDS * 30, TimeFormat.TIME_FORMAT_MS, STREAM_ID)
        end_time = utils.get_formatted_time(constants.SECONDS * 90, TimeFormat.TIME_FORMAT_MS, STREAM_ID)
        response = destructive_utils.create_recording_des(start_time, end_time)

        recording_id = response[RecordingAttribute.RECORDING_ID]
        recording_response = rio.find_recording(recording_id).json()
        LOGGER.debug("Recording response=%s", recording_response)

        web_service_obj = response[RecordingAttribute.WEB_SERVICE_OBJECT]
        is_valid, desc = validate_recordings.validate_recording_end_state(
            recording_id, [RecordingStatus.INCOMPLETE, RecordingStatus.COMPLETE], web_service_obj=web_service_obj,
            end_time=end_time)

        assert is_valid, desc
        if is_valid and desc == RecordingStatus.COMPLETE:
            is_valid, error = validate_recordings.validate_playback_using_vle(
                recording_id, VALIDATOR_TYPE=vle_validators_configuration.PLAYBACK_VALIDATION_COMPLETE)
            assert is_valid, error

        # executing the revert command to undo the destructive commands
        for mce_node in mce_nodes:
            ssh_client = utils.get_ssh_client(COMPONENT_NAME, COMPONENT_USERNAME, component_ip=mce_node[Component.IP])
            if rev_cmds[mce_ip]:
                rev_effective, error = destructive_utils.exec_rev_cmd(COMPONENT_NAME, ssh_client, mce_ip,
                                                                      DestructiveTesting.IFB_INTERFACE, rev_cmds[mce_ip],
                                                                      destructive.MCE_JOB_IDS)
            else:
                rev_effective, error = destructive_utils.is_rev_effective(ssh_client, DestructiveTesting.IFB_INTERFACE)
            assert rev_effective, error

        # running sanity test to check if the setup is back to normal after reverting the commands
        test_rtc9723_tc_rec_001_future_start_time_future_end_time(STREAM_ID)
    finally:
        if ssh_client:
            ssh_client.close()
        if web_service_obj:
            web_service_obj.stop_server()
        if response:
            response = a8.delete_recording(response[RecordingAttribute.RECORDING])
            LOGGER.debug("Recording clean up status code=%s", response.status_code)
