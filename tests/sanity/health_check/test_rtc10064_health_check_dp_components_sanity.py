import logging

import pytest
import os

import helpers.constants as constants
from helpers.vmr.vmr_helper import get_pod_details
from helpers.constants import Component
from helpers.utils import (
    get_custom_ssh_client, get_configInfo, get_spec_config, is_standalone,
    is_v2pc, get_ssh_client)
from helpers.constants import TestLog


pytestmark = pytest.mark.recording
LOGGER = logging.getLogger(os.environ.get("PYTEST_XDIST_WORKER", TestLog.TEST_LOGGER))


def test_rtc10064_health_check_dp_components():
    config_info = get_configInfo()
    vmr_pods = get_pod_details(constants.Component.VMR)
    running_pods = list()
    unavailable_pods = list()
    mandatory_pods = ['a8-updater', 'archive-agent', 'api', 'dash-origin', 'health-agent', 'health-monitor',
                      'manifest-agent', 'nsa', 'reconstitution-agent', 'recorder-manager',
                      'segment-recorder', 'ui', 'zookeeper', 'bmw', 'sats-server']
    for i in vmr_pods.items:
        pod_name = i.metadata.name
        running_pods.append(pod_name)
        LOGGER.debug("%s\t%s\t%s" % (i.status.pod_ip, pod_name, i.status.phase))
        if i.status.phase not in ["Succeeded", "Running"]:
            raise Exception("Testcase failed: Some of the pods in VMR are not in running state.")

    for pod in mandatory_pods:
        verification_data = ','.join(rp for rp in running_pods if pod in rp)
        if len(verification_data) == 0:
            msg = '%s pod is unavailable' % pod
            LOGGER.error(msg)
            unavailable_pods.append(pod)

    if len(unavailable_pods) > 0:
        raise Exception('Following pods are unavailable: %s' % ','.join(unavailable_pods))

    # Check the status of MCE pods:
    mce_confg = get_spec_config(Component.MCE)
    mce_username = mce_confg[Component.MCE_USERNAME]
    if is_standalone(Component.MCE):
        mce_ip = mce_confg['node1']['ip']
        mce_key = mce_confg['sshkey']
        ssh_client = get_custom_ssh_client(ip=mce_ip, username=mce_username, key_path=mce_key)
        stdin, stdout, stderr = ssh_client.exec_command("/sbin/service mce-app status", get_pty=False)
        data = stdout.readlines()
        ssh_client.close()
        for line in data:
            LOGGER.debug(line)
            each_service = line.split()
            if len(each_service) > 2:
                if each_service[0] == 'asperacentral':
                    continue
                assert 'RUNNING' == str(each_service[1]), 'ERROR: %s service is in %s state' % (each_service[0], each_service[1])
        LOGGER.debug('MCE IP: ' + str(mce_ip))
        LOGGER.debug('MCE is up and running...')
        LOGGER.debug("EXIT CODE: " + str(stdout.channel.recv_exit_status()))

    if is_v2pc(Component.MCE):
        mce_ip = mce_confg[Component.VIRTUAL_IP]
        ssh_client = get_ssh_client(Component.MCE, mce_username)
        stdin, stdout, stderr = ssh_client.exec_command("/sbin/service mce-app status", get_pty=False)
        data = stdout.readlines()
        ssh_client.close()
        for line in data:
            LOGGER.debug(line)
            each_service = line.split()
            if len(each_service) > 2:
                if each_service[0] == 'asperacentral':
                    continue
                assert 'RUNNING' == str(each_service[1]), 'ERROR: %s service is in %s state' % (each_service[0], each_service[1])
        LOGGER.debug('MCE IP: ' + str(mce_ip))
        LOGGER.debug('MCE is up and running...')
        LOGGER.debug("EXIT CODE: " + str(stdout.channel.recv_exit_status()))

    # Check the status of MPE pods:
    mpe_confg = get_spec_config(Component.MPE)
    if is_standalone(Component.MPE):
        # namespace
        mpe_pods = get_pod_details(mpe_confg[Component.NAMESPACE])
        running_pods = list()
        unavailable_pods = list()
        mandatory_pods = ['mpe', ]
        for i in mpe_pods.items:
            pod_name = i.metadata.name
            running_pods.append(pod_name)
            LOGGER.debug("%s\t%s\t%s" % (i.status.pod_ip, pod_name, i.status.phase))
            if i.status.phase not in ["Succeeded", "Running"]:
                raise Exception("Testcase failed: Some of the pods in VMR are not in running state.")

        for pod in mandatory_pods:
            verification_data = ','.join(rp for rp in running_pods if pod in rp)
            if len(verification_data) == 0:
                msg = '%s pod is unavailable' % pod
                LOGGER.error(msg)
                unavailable_pods.append(pod)

        if len(unavailable_pods) > 0:
            raise Exception('Following pods are unavailable: %s' % ','.join(unavailable_pods))

    if is_v2pc(Component.MPE):
        mpe_ip = mpe_confg['node1'][Component.IP]
        mpe_username = mpe_confg[Component.MPE_USERNAME]
        ssh_client = get_custom_ssh_client(mpe_ip, username=mpe_username, key_path=mpe_confg[Component.SSH_KEY])
        stdin, stdout, stderr = ssh_client.exec_command("systemctl status mpe.service", get_pty=False)
        data = stdout.read()
        ssh_client.close()
        LOGGER.debug(str(data))
        if 'active' in str(data) and 'running' in str(data):
            LOGGER.debug('MPE service is up and running...')
        else:
            raise Exception('ERROR: MPE service is not up and running...')
        LOGGER.debug('MPE IP:' + str(mpe_ip))
        LOGGER.debug("EXIT CODE: " + str(stdout.channel.recv_exit_status()))


    # Check COS - CLM and Cassandra service:
    cos_confg = get_spec_config(Component.COS)
    cos_ip = cos_confg['node1']['ip']
    cos_username = cos_confg['user']
    cos_password = cos_confg['pass']
    ssh_client = get_custom_ssh_client(ip=cos_ip, username=cos_username, password=cos_password)
    stdin, stdout, stderr = ssh_client.exec_command("/sbin/service clm status", get_pty=True)
    data = stdout.read()
    ssh_client.close()
    LOGGER.debug(str(data))
    if 'passing' in str(data):
        LOGGER.debug('COS: CLM service is running...')
    else:
        raise Exception('ERROR: COS: CLM service is not up and running...')
    LOGGER.debug('COS IP:' + str(cos_ip))
    LOGGER.debug("EXIT CODE: " + str(stdout.channel.recv_exit_status()))
    
    # Check CMC service:
    cmc_confg = get_spec_config(Component.CMC)
    cmc_ip = cmc_confg['ip']
    cmc_username = cmc_confg['user']
    cmc_password = cmc_confg['pass']
    ssh_client = get_custom_ssh_client(ip=cmc_ip, username=cmc_username, password=cmc_password)
    stdin, stdout, stderr = ssh_client.exec_command("/sbin/service cmc_aicc status", get_pty=True)
    data = stdout.read()
    ssh_client.close()
    LOGGER.debug(str(data))
    if 'cmc_aicc is running' in str(data):
        LOGGER.debug('CMC_AICC service is up and running...')
    else:
        raise Exception('ERROR: CMC_AICC service is not up and running...')
    LOGGER.debug('CMC IP: ' + str(cmc_ip))
    LOGGER.debug("EXIT CODE: " + str(stdout.channel.recv_exit_status()))
    
    # Check MEMSQL service:
    memsql_ip = config_info[Component.MEMSQL]['ip']
    memsql_username = config_info[Component.MEMSQL]['user']
    memsql_password = config_info[Component.MEMSQL]['pass']
    ssh_client = get_custom_ssh_client(ip=memsql_ip, username=memsql_username, password=memsql_password)
    stdin, stdout, stderr = ssh_client.exec_command("memsql-ops status", get_pty=True)
    data = stdout.read()
    ssh_client.close()
    if 'MemSQL Ops is running' in str(data):
        LOGGER.debug('MEMSQL-OPS is up and running...')
    else:
        raise Exception('ERROR: MEMSQL-OPS is not up and running...')
    LOGGER.debug(str(data))
    LOGGER.debug('MEMSQL IP: ' + str(memsql_ip))
    LOGGER.debug("EXIT CODE: " + str(stdout.channel.recv_exit_status()))
