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
@pytest.mark.skip(reason = "Defect CSCvd85052 created for this failed Test case")
def test_rtc9804_tc_des_011_mce_ni_packet_latency():
    """
    Introduce the latency on each packet on the outgoing MCE Interface, trigger a recording and verify if the
    recording is successful
    """
    ssh_client = None
    response = None
    web_service_obj = None
    start_duration = 30
    try:
        rev_cmds = {}
        mce_nodes = v2pc.get_app_worker_nodes(MCE_INSTANCE, COMPONENT_NAME)

        for mce_node in mce_nodes:
            mce_data_out = mce_node[Interface.DATA_OUT]
            mce_ip = mce_node[Component.IP]

            ssh_client = utils.get_ssh_client(COMPONENT_NAME, COMPONENT_USERNAME, component_ip=mce_ip)

            # deleting the previously scheduled jobs by other test cases, in order not to tamper with the current test case
            destructive_utils.delete_scheduled_job(COMPONENT_NAME, ssh_client, mce_ip, destructive.MCE_JOB_IDS)

            rev_cmds[mce_ip] = destructive_utils.schedule_rev_cmd(ssh_client, mce_data_out, mce_ip,
                                                                      destructive.MCE_JOB_IDS, constants.MINUTES * 10)
            des_cmd = DestructiveTesting.PACKET_LATENCY_OUTGOING_INTERFACE.format(mce_data_out,
                                                                                  DestructiveTesting.PACKET_LATENCY)
            des_cmd = destructive_utils.get_outgoing_tc_cmd(mce_data_out, des_cmd)

            # expected outcome after the destructive commands are run
            expected_result = {DestructiveTesting.DELAY: DestructiveTesting.PACKET_LATENCY,
                               DestructiveTesting.DST: DestructiveTesting.NETWORK}
            is_des_effective, error = destructive_utils.exec_des_cmd(ssh_client, mce_data_out, des_cmd, expected_result)
            assert is_des_effective, error

        start_time = utils.get_formatted_time(constants.SECONDS * start_duration, TimeFormat.TIME_FORMAT_MS, STREAM_ID)
        end_time = utils.get_formatted_time(constants.SECONDS * 90, TimeFormat.TIME_FORMAT_MS, STREAM_ID)
        response = destructive_utils.create_recording_des(start_time, end_time)

        web_service_obj = response[RecordingAttribute.WEB_SERVICE_OBJECT]
        recording_id = response[RecordingAttribute.RECORDING_ID]
        is_valid, error = validate_recordings.validate_recording(recording_id, web_service_obj)
        assert is_valid, error

        is_valid, error = validate_recordings.validate_playback(recording_id)
        assert is_valid, error

        # executing the revert command to undo the destructive commands
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
