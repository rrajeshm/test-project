import logging
import time
from collections import OrderedDict

import pytest
import yaml

import helpers.constants as constants
import helpers.destructive.utils as destructive_utils
import helpers.utils as utils
import helpers.v2pc.v2pc_helper as v2pc_helper
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

pytestmark = pytest.mark.cos_destructive

LOGGER = logging.getLogger(TestLog.TEST_LOGGER)

CONFIG_INFO = yaml.load(pytest.config.getoption(constants.GLOBAL_CONFIG_INFO))

COMPONENT_NAME = Component.COS
COMPONENT_USERNAME = CONFIG_INFO[COMPONENT_NAME][Component.COS_USER]

STREAM_1_CONFIG = CONFIG_INFO[Component.V2PC][Component.STREAM_PROFILES][Component.STREAM_1]
STREAM_ID = str(STREAM_1_CONFIG[Component.ID])

COS_PASSWORD = CONFIG_INFO[Component.COS][Component.COS_PASS]


@utils.test_case_logger
def test_rtc9806_tc_des_013_cos_ni_block_inprogress_recording():
    """
    Block Incoming COS network interface after recording starts and unblock it before the recording ends,
    Validate the recording state against INCOMPLETE and number of available segments recorded.
    """
    ssh_client = None
    response = None
    web_service_obj = None
    start_duration = 30
    block_trigger_time = 30
    block_duration = 60
    rev_if_dict = OrderedDict()
    component_dict = OrderedDict()
    des_dict = OrderedDict()
    des_cmd_dict = OrderedDict()
    ifbcount = 0
    cos_nodes = v2pc_helper.get_cos_node_data()

    try:
        for node in cos_nodes:
            comp_ip = node[Component.IP]

            ssh_client = utils.get_ssh_client(COMPONENT_NAME, COMPONENT_USERNAME, component_ip=comp_ip,
                                              password=COS_PASSWORD)

            # deleting the previously scheduled jobs, in order not to tamper with the current test case
            destructive_utils.delete_scheduled_job(COMPONENT_NAME, ssh_client, comp_ip, destructive.COS_JOB_IDS)

            # interface takes only data interfaces
            for interface in node[Interface.INTERFACES]:
                cos_data_in = interface

                ifb_interface = DestructiveTesting.IFB_INTERFACE + str(ifbcount)

                rev_cmd = destructive_utils.schedule_rev_cmd(ssh_client, cos_data_in, comp_ip,
                                                             destructive.COS_JOB_IDS,
                                                             constants.MINUTES * 2, ifb_interface)

                # Storing the revert command with its respective interface
                rev_if_dict[cos_data_in] = rev_cmd

                des_cmd = DestructiveTesting.PACKET_LOSS_INCOMING_INTERFACE.format(ifb_interface,
                                                                                   DestructiveTesting.PACKET_LOSS_BLOCK)

                des_cmd = destructive_utils.get_incoming_tc_cmd(cos_data_in, des_cmd, ifb_interface)

                # Storing the destructive command with its respective interface
                des_dict[cos_data_in] = des_cmd
                ifbcount += 1

            # Storing Interfaces and Revert Commands with its respective Component IP
            component_dict[comp_ip] = rev_if_dict
            # Storing Interfaces and Destructive Commands with its respective Component IP
            des_cmd_dict[comp_ip] = des_dict

        # Create a recording to block in progress recording

        start_time = utils.get_formatted_time(constants.SECONDS * start_duration, TimeFormat.TIME_FORMAT_MS, STREAM_ID)
        end_time = utils.get_formatted_time(constants.SECONDS * 210, TimeFormat.TIME_FORMAT_MS, STREAM_ID)
        response = destructive_utils.create_recording_des(start_time, end_time)

        web_service_obj = response[RecordingAttribute.WEB_SERVICE_OBJECT]
        is_valid, error = validate_recordings.validate_notification(web_service_obj, constants.RecordingStatus.STARTED,
                                                                    constants.SECONDS * start_duration)

        assert is_valid, error

        time.sleep(constants.SECONDS * block_trigger_time)

        ifbcount = 0
        for key, values in des_cmd_dict.items():
            for component_ips, des_cmd in values.items():
                # expected outcome after the destructive commands are run

                ifb_interface = DestructiveTesting.IFB_INTERFACE + str(ifbcount)
                expected_result = {DestructiveTesting.LOSS: DestructiveTesting.PACKET_LOSS_BLOCK,
                                   DestructiveTesting.SRC: DestructiveTesting.NETWORK}
                is_des_effective, error = destructive_utils.exec_des_cmd(ssh_client, ifb_interface, des_cmd,
                                                                         expected_result)
                assert is_des_effective, error
                ifbcount += 1

        time.sleep(constants.SECONDS * block_duration)
        # executing the revert command to undo the destructive commands
        for component_ip, values in component_dict.items():
            for interface, rev_cmds in values.items():
                destructive_utils.exec_rev_cmd(COMPONENT_NAME, ssh_client, component_ip, interface, rev_cmds,
                                               destructive.COS_JOB_IDS)

        recording_id = response[RecordingAttribute.RECORDING_ID]
        is_valid, error = validate_recordings.validate_recording_end_state(
            recording_id, [RecordingStatus.INCOMPLETE], web_service_obj=web_service_obj,
            end_time=end_time)

        assert is_valid, error

        is_valid, error = validate_recordings.validate_segments_threshold_storage(
            recording_id, constants.SECONDS * block_duration)

        assert is_valid, error
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
