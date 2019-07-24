import logging
import time
import os
import requests
import paramiko
import pytest
import yaml

import helpers.utils as utils
import helpers.setup as test_setup
import helpers.constants as constants
from helpers.constants import Component
from helpers.constants import TestLog
from helpers.constants import TimeFormat
import validators.validate_common as validate_common
import validators.validate_recordings as validate_recordings
import validators.validate_storage as validate_storage
from helpers.constants import Cos
import helpers.notification.utils as notification_utils
import helpers.vmr.a8.a8_helper as a8
import helpers.vmr.rio.api as rio
import helpers.models.recording as recording_model

pytestmark = pytest.mark.destructive

LOGGER = logging.getLogger(os.environ.get("PYTEST_XDIST_WORKER", TestLog.TEST_LOGGER))

CONFIG_INFO = yaml.load(pytest.config.getoption(constants.GLOBAL_CONFIG_INFO))

STREAM_1_CONFIG = CONFIG_INFO[Component.V2PC][Component.STREAM_PROFILES][Component.STREAM_1]
STREAM_ID = str(STREAM_1_CONFIG[Component.ID])


@utils.test_case_logger
def test_tc_des_016_vmr_ntp_out_of_sync():
    LOGGER.info("vmr_ntp_out_of_sync test case...")
    recording = None
    web_service_obj = None
    timeout = int(os.environ.get(STREAM_ID))

    ntp_server_v2pc = CONFIG_INFO[Component.V2PC][Component.NTP]
    india_ntp = '1.in.pool.ntp.org'
    ntp_synchronization_time = 300

    # Initiate the recording
    try:
        start_time = utils.get_formatted_time((constants.SECONDS * 30) + timeout, TimeFormat.TIME_FORMAT_MS, stream)
        end_time = utils.get_formatted_time((constants.SECONDS * 90) + timeout, TimeFormat.TIME_FORMAT_MS, stream)
        recording = recording_model.Recording(StartTime=start_time, EndTime=end_time, StreamId=STREAM_ID)
        recording_id = recording.get_entry(0).RecordingId
        web_service_obj = notification_utils.get_web_service_object(recording_id)
        recording.get_entry(0).UpdateUrl = web_service_obj.get_url()
        LOGGER.debug("Recording instance created=%s", recording.serialize())
        response = a8.create_recording(recording)
        is_valid, error = validate_common.validate_http_response_status_code(response, requests.codes.no_content)
        assert is_valid, error
        time.sleep(40)

        # Configure to the right ntp server in VMR
        cmd = "sed -i -E 's/" + ntp_server_v2pc + "/" + india_ntp + "/g' /etc/ntp.conf"
        update_vmr_ntp_server(cmd)
        time.sleep(ntp_synchronization_time)

        # Validate recording is incomplete
        is_valid, error = validate_recordings.validate_recording(recording_id, web_service_obj)
        LOGGER.info(is_valid)
        LOGGER.info(error)
        assert is_valid, error

        response = rio.find_recording(recording_id).json()
        is_valid, error = validate_storage.validate_recording_in_storage(response, Cos.ACTIVE_STORAGE,
                                                                         Cos.RECORDING_STORED)

        assert is_valid, error

        is_valid, error = validate_recordings.validate_playback(recording_id)

        assert is_valid, error
    finally:
        # revert back the ntp to the original value in vmr
        cmd = "sed -i -E 's/" + india_ntp + "/" + ntp_server_v2pc + "/g' /etc/ntp.conf"
        update_vmr_ntp_server(cmd)
        time.sleep(ntp_synchronization_time)
        web_service_obj.stop_server()
        response = a8.delete_recording(recording)
        LOGGER.debug("Recording clean up status code=%s", response.status_code)
        # Check whether all the components are synchronized.
        times_are_synchronized, error = test_setup.are_times_synchronized()
        if not times_are_synchronized:
            pytest.fail(error)


def update_vmr_ntp_server(command):
    try:
        name = Component.VMR
        vmr_ip = CONFIG_INFO[name][Component.A8][Component.IP]
        vmr_name = CONFIG_INFO[Component.VMR][Component.VMR_USERNAME]
        vmr_password = CONFIG_INFO[Component.VMR][Component.VMR_PASSWORD]
        ssh_connect = paramiko.SSHClient()
        ssh_connect.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_connect.connect(hostname=vmr_ip, username=vmr_name, password=vmr_password)
        ssh_connect.invoke_shell()
        ssh_connect.exec_command(command)
        stp_cmd = "service ntpd stop"
        ssh_connect.exec_command(stp_cmd)
        time.sleep(120)  # wait time for ntp service to get stopped completely otherwise it is in stopping state.
        srt_cmd = "service ntpd start"
        ssh_connect.exec_command(srt_cmd)
        ssh_connect.close()

    except Exception as e:
        LOGGER.info(str(e))
