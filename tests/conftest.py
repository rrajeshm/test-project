"""
Configuration options for pytest
"""
import logging
import warnings
import os
import re
import sys
import ast
import requests
from collections import OrderedDict
from datetime import datetime

import py
import pytest
import yaml
import operator

from kubernetes import config
from test_manifests import test_manifest

import helpers.setup as test_setup
import helpers.teardown as test_teardown
import helpers.utils as utils
import helpers.constants as constants

from helpers import dp_lib
from helpers.constants import Component, VMR_KUBECONFIG
from helpers.constants import PytestOptions
from helpers.constants import RecordingAttribute
from helpers.constants import TestLog, Stream_tags
from helpers.constants import TestDirectory, TestReport

LOGGER = utils.get_logger()
debug_logger = utils.get_logger(TestLog.DEBUG_LOGGER)
# log('test_manifest.tests=%s' % test_manifest.tests)
# debug_logger.info('test_manifest.tests=%s' % test_manifest.tests)

prefix_on_log_errors = 'XX' * 25
suffix_on_log_errors = 'XX' * 25

orders_map = {
    'first': 0,
    'second': 1,
    'third': 2,
    'fourth': 3,
    'fifth': 4,
    'sixth': 5,
    'seventh': 6,
    'eighth': 7,
    'last': -1,
    'second_to_last': -2,
    'third_to_last': -3,
    'fourth_to_last': -4,
    'fifth_to_last': -5,
    'sixth_to_last': -6,
    'seventh_to_last': -7,
    'eighth_to_last': -8,
}


def pytest_addoption(parser):
    """
    Custom command line arguments for pytest
    """
    parser.addoption("--configInfo", action="store", default=None, help="Information obtained by parsing the "
                                                                        "configuration (.yaml) files.")
    parser.addoption("--streamsInfo", action="store", default=None, help="Information obtained by parsing the "
                                                                        " stream configuration from (.yaml) files.")
    parser.addoption('--skipData', action="store")
    parser.addoption('--labName', action="store", default=constants.LABNAME)
    parser.addoption('--pvt', action="store", default="common")


def is_master(config):
    """True if the code running the given pytest.config object is running in a xdist master
    node or not running xdist at all.
    """
    return not hasattr(config, 'slaveinput')


def execute_pre_check():
    # validated oc CLI
    result = utils.execute_local('oc')
    if result[0]:
        pytest.fail("OpenShift CLI: oc command is not installed in this system")

    # validate kubeconfig for VMR
    base_path = os.path.join(utils.get_base_dir_path(), TestDirectory.CONFIG_DIR)
    kubeconfig_path = os.path.join(base_path, VMR_KUBECONFIG)

    if os.path.isfile(kubeconfig_path) is False:
        pytest.fail("VMR KUBECONFIG FILE NOT FOUND: %s" %kubeconfig_path)

    os.environ["KUBECONFIG"] = kubeconfig_path
    result = utils.execute_local('oc project vmr')
    if result[0]:
        pytest.fail("Invalid VMR KUBECONFIG: %s" % kubeconfig_path)


def pytest_configure(config):
    conf_info = yaml.load(config.getoption(constants.GLOBAL_CONFIG_INFO))
    TestDirectory.CONFIG_DIR = conf_info['test_config_dir']

    # Execute PreCheck
    execute_pre_check()

    # Let the master node book-keep a list of streams for the slave nodes to work on.
    if is_master(config):
        config.streamInfo = list(yaml.load(config.getoption(constants.GLOBAL_STREAM_INFO)))
        LOGGER.info("config.streamInfo {0} ".format(config.streamInfo))


def pytest_unconfigure(config):
    if is_master(config):
        test_teardown.archive_logs()


def pytest_configure_node(node):
    # The master assigns the slaves the stream to work on and pytest-xdist will transfer to the subprocess
    node.slaveinput[RecordingAttribute.STREAM_ID] = node.config.streamInfo.pop()


@pytest.hookimpl(tryfirst=True)
def pytest_xdist_setupnodes(config, specs):
    """ called before any remote node is set up. """
    logger = logging.getLogger(TestLog.TEST_LOGGER)

    stream_list_reverse = config.streamInfo[::-1]
    log_dir = os.path.join(utils.get_base_dir_path(), TestDirectory.OUTPUT_DIR)
    for i,stream in enumerate(stream_list_reverse):
        specs[i].id = stream
        log_file = os.path.join(log_dir, stream+'.log')
        msg = "All log statements related to stream: %s will be updated to %s" % (stream, log_file)
        logging.info(msg)
        # write in master log file about stream logging details
        logger.info(msg)


def pytest_sessionstart(session):    
    pytest.suite_start_time = datetime.utcnow()
    
    pytest.tc_data = OrderedDict()
    pytest.tc_data['testcases'] = OrderedDict()
    pytest.tc_data['summary'] = OrderedDict()
    pytest.tc_data['summary']['counts'] = OrderedDict()
    pytest.tc_data['summary']['percentage'] = OrderedDict()    
        
    test_setup.init_logger(os.environ.get("PYTEST_XDIST_WORKER", TestLog.TEST_LOGGER)) 
    test_setup.init_logger(TestLog.DEBUG_LOGGER, TestLog.LOG_FORMAT_DEBUG)
        
    rally_result_file = os.path.join(utils.get_base_dir_path(), TestReport.RALLY_RESULT_FILE)
    if os.path.isfile(rally_result_file):
        os.unlink(rally_result_file)


@pytest.fixture(scope="session", autouse=True)
def setup(request):
    """
    Set up the environment for the test run. This will be executed by default automatically before every test run.
    :param request: the incoming context
    """

    def teardown():
        """
        Clean up the environment post the test run. This will be executed when all the tests are executed.
        """
        test_setup.prepare_test_summary(request)

    request.addfinalizer(teardown)

    base_path = os.path.join(utils.get_base_dir_path(), TestDirectory.CONFIG_DIR)
    kubeconfig_path = os.path.join(base_path, VMR_KUBECONFIG)
    config.load_kube_config(os.path.join(os.environ["HOME"], kubeconfig_path))

    # to cleanup log and change vle_default.conf file interface value based upon the system interface name"
    utils.vle_log_cleanup()
    utils.vle_default_config_interface_change()
    
    times_are_synchronized, error = test_setup.are_times_synchronized()
    if not times_are_synchronized:
        pytest.fail(error)

    LOGGER.info("All components including the test machine are time synchronized as required")

    playback_host_resolved, error = test_setup.is_playback_host_resolved()
    if not playback_host_resolved:
        pytest.fail(error)

    streams_info, drift_avail, stream_drift_info, drift_applicable = test_setup.find_live_point_drift()
    generic_direct_config = utils.get_direct_config()
    generic_config = utils.get_spec_config()

    if not drift_applicable:
        warnings.warn(UserWarning(stream_drift_info + 'Not applicable drift streams removed.'))

    # Fail if time drift present in stream, but drift handle is False
    if not(generic_direct_config.get(Component.DRIFT_HANDLE, False)) and drift_avail:
        message = stream_drift_info + " stream has drift but drift handle is set to False, hence failing the test execution"
        LOGGER.error("Stream -> drift: %s" % stream_drift_info)
        LOGGER.error(message)
        pytest.exit(message)

    # Expose the stream details as environment variable
    if generic_direct_config.get(Component.DRIFT_HANDLE, False):
        status = utils.set_pytest_stream_info(streams_info)


    if utils.is_v2pc():
        v2pc_auth_token = utils.get_v2pc_api_auth_token()
        if not v2pc_auth_token:
            pytest.fail("Unable to fetch V2PC authorization token")
        constants.V2pc.auth_token = v2pc_auth_token


# This triggers streams to 1 Tc
def pytest_generate_tests(metafunc):
    if not os.environ.get("PYTEST_XDIST_WORKER"):
        # this will parametrize stream when pytest running in non distributed mode
        stream_info = utils.get_streams()

        if 'stream' in metafunc.fixturenames:
            stream_info = utils.fetch_streams(Stream_tags.GENERIC)
            if not stream_info:
                pytest.fail("Streams unavailable to proceed.")
            metafunc.parametrize("stream", stream_info, ids=stream_info)


@pytest.fixture(scope="session")
def stream(request):
    # this fixture will be used when pytest running in distributed mode
    stream_id = None
    slave_input = getattr(request.config, "slaveinput", None)
    stream_id = slave_input[RecordingAttribute.STREAM_ID]
    return stream_id


def pytest_cmdline_preparse(args):
    # Check whether pytest-xdist is installed
    if not PytestOptions.XDIST_PLUGIN in sys.modules:
        # If pytest_xdist is not installed, remove its related arguments that were added when they were parsed earlier.
        # As without this plugin, if executed with the below arguments, an error will be reported.
        if PytestOptions.XDIST_DIST in args:			
	    index = args.index(PytestOptions.XDIST_DIST)
	    args[:] = args[:index] + args[index + 2:] # Remove the option and its corresponding value
	if PytestOptions.XDIST_TX in args:
	    args[:] = args[:index] + args[index + 2:]
	LOGGER.debug("%s is not installed. Cannot parallelize tests.", PytestOptions.XDIST_PLUGIN)


def t_pytest_runtest_setup(item):
    """Skip tests if they are marked with respect to stream"""
    skipData = item.config.getvalue('skipData') or {}
    skipData = skipData and ast.literal_eval(skipData) or skipData

    # identify which stream undergoes for testing currently
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    if worker == None:
       if hasattr(item, 'callspec'):
           worker = item.callspec.getparam('stream')

    if worker in skipData:
        for _marker in skipData[worker]:
            if item.get_marker(_marker):
                py.test.skip('The %s tests skipped for stream: %s' %(_marker, worker))


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    # execute all other hooks to obtain the report object
    outcome = yield
    rep = outcome.get_result()

    # we only look at actual failing test calls, not setup/teardown

    if rep.when == "call":
        
        item.duration = rep.duration
        
        if rep.failed:
            item.status = "Fail"
            LOGGER.error("%s %s %s" %(prefix_on_log_errors, '  E R R O R  ', suffix_on_log_errors))            
            LOGGER.error("Test failed - Traceback")            
            LOGGER.error(rep.longreprtext)
            
            testLocals = str(rep.longrepr.reprtraceback.reprentries[0].reprlocals)
            testDict = {}
            
            for x in re.findall(r'.*=.*', testLocals):
                result = re.search('([\w\s\t]+)=(.*)', x)
                if result:
                    k, v = result.group(1), result.group(2)
                    k, v = k.strip(), v.lstrip()

                    if k in ['TC', 'US', 'message']:
                        testDict[k] = ast.literal_eval(v)
                    else:
                        testDict[k] = v
                
            _issue = re.search(r'> (.*)', rep.longreprtext).group(1)
            _error = "\n".join([ x.strip() for x in re.findall(r'E\s(.*)', rep.longreprtext)])
            
            item.error = _issue.strip() + "\n" + _error            
            item.message = testDict.get('message', "")
          
        else:
            item.status = "Pass"
            item.message = "TestCase Passed"
            item.error = ""


def pytest_runtest_setup(item):
    start_time = datetime.utcnow()
    item.start_time = start_time
    
    started_message = "\n%s %s %s %s %s\n" %(str(start_time),'#' * 30, "STARTED : ", item.name, '#' * 30)
    print started_message
    LOGGER.info(started_message)
    skipData = item.config.getvalue('skipData') or []
    skipData = skipData and ast.literal_eval(skipData) or skipData

    for _marker in skipData:
        if item.get_marker(_marker):            
            skipped_message = "\n%s %s %s %s\n" %('#' * 30, "SKIPPED : ", item.name, '#' * 30)
            print skipped_message
            logger.info(skipped_message)          
            item.status = "Skip"
            item.message = "TestCase Skipped"
            item.error = ""
            item.duration = 0
            py.test.skip('The %s tests skipped' %(_marker))
                            
        
def pytest_runtest_teardown(item):
    item.end_time = datetime.utcnow()
    ended_message = "\n%s %s %s %s %s\n" % (str(item.end_time), '#' * 30, "ENDED : ", item.name, '#' * 30)
    print ended_message
    LOGGER.info(ended_message)

    TC = re.findall(r'(tc\d+)', item.name)
    item.TC = TC
    status = getattr(item, "status", "Skip")
    if status == "Skip":
        message = getattr(item, "message", "Skipped")
        status = getattr(item, "status", "Skip")
        duration = getattr(item, "duration", 0)
        error = getattr(item, "error", "framework related error")
        for _tc in item.TC:
            pytest.tc_data['testcases'][item.name] = {
                "name": item.name,
                "message": message,
                "status": status,
                "duration": duration,
                "end_time": str(item.end_time),
                "id": _tc,
                "error": error,
                "file": os.path.join(utils.get_base_dir_path(), item.location[0])
            }
            try:
                pytest.tc_data['testcases'][item.name] = {
                    "start_time": str(item.start_time)
                }
            except:
                pass
    else:
        message = getattr(item, "message", "Failed")
        status = getattr(item, "status", "Fail")
        duration = getattr(item, "duration", 0)
        error = getattr(item, "error", "framework related error")
        for _tc in item.TC:
            pytest.tc_data['testcases'][item.name] = {
                "name": item.name,
                "message": message,
                "status": status,
                "duration": duration,
                "start_time": str(item.start_time),
                "end_time": str(item.end_time),
                "id": _tc,
                "error": error,
                "file": os.path.join(utils.get_base_dir_path(), item.location[0])
            }


# test_manifest is added by pipeline_test/cdvr_basic.py or cdvr_sanity.py.
# specifically it gets copied from test_manifests folder into the tests folder
# so it can get imported here..
# import test_manifest
# debug_logger = utils.get_logger(TestLog.DEBUG_LOGGER)
# log('test_manifest.tests=%s' % test_manifest.tests)
# debug_logger.info('test_manifest.tests=%s' % test_manifest.tests)

# note: HLA specifies this should be a colon ':' but pytest seems to require markers to begin with alphabetic
COLON = ':'
CUSTOM_IDENTIFIER = 'fmap_' # = COLON

FEATUREMAP_KEY = 'featureMap'
REQUIREMENT_PREFIXES = ['MF', 'CF', 'US', CUSTOM_IDENTIFIER]

## IMPORTANT: the logic below is for pytest == 3.3.0 as defined in requirements.txt
## Data structures for another pytest version may not be the same so
## if you change the pytest version, you may have to change this function!!
## ================================================================================
@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(config, items):

    # log('enter pytest_collection_modifyitems, len(items)=%s' % len(items))
    generic_confg = utils.get_direct_config()
    if not generic_confg.get(Component.USE_MANIFEST):
        grouped_items = {}
        for item in items:
            for mark_name, order in orders_map.items():
                mark = item.get_closest_marker(mark_name)
                if mark:
                    item.add_marker(pytest.mark.run(order=order))
                    break

            mark = item.get_closest_marker('run')
            if mark:
                order = mark.kwargs.get('order')
            else:
                order = None

            grouped_items.setdefault(order, []).append(item)
        sorted_items = []
        unordered_items = [grouped_items.pop(None, [])]
        start_list = sorted((i for i in grouped_items.items() if i[0] >= 0),
                            key=operator.itemgetter(0))
        end_list = sorted((i for i in grouped_items.items() if i[0] < 0),
                          key=operator.itemgetter(0))
        sorted_items.extend([i[1] for i in start_list])
        sorted_items.extend(unordered_items)
        sorted_items.extend([i[1] for i in end_list])
        items[:] = [item for sublist in sorted_items for item in sublist]
        return

    debug_logger.info('enter pytest_collection_modifyitems, len(items)=%s' % len(items))
    collected = [] # list of all testcases in both the repo and manifest
    fmapped = {} # dict of testcases with a featureMap
    debug_logger.info("test_manifest.tests {0}".format(test_manifest.tests))
    for item in items:
        # log('hook item.nodeid=%s' % item.nodeid)
        debug_logger.info('hook item.nodeid=%s' % item.nodeid)
        collected.append(item.nodeid)

        # get base_nodeid by truncating item.nodeid at the opening bracket ('['), if it exists
        # note: test_manifests/test_manifest.py contains a list of these truncated ("base") nodeid's
        idx = item.nodeid.find('[')
        base_nodeid = item.nodeid[:idx] if idx >= 0 else item.nodeid
        if not (base_nodeid in test_manifest.tests):
            # log('hook xfail test=%s' % item.nodeid)
            debug_logger.info('hook xfail test=%s' % item.nodeid)
            item.add_marker(pytest.mark.xfail)

        if TestLog.FEATUREMAP_KEY in item._obj.func_code.co_varnames:
            idxfm = item._obj.func_code.co_varnames.index(TestLog.FEATUREMAP_KEY)
            if idxfm != item._obj.func_code.co_argcount:
                # log( '[ERROR] testcase %s declares a featureMap but its not first so will be ignored (move featureMap before "%s").' %
                #         (item.nodeid, item._obj.func_code.co_varnames[idxfm-1]) )
                debug_logger.error( '[ERROR] testcase %s declares a featureMap but its not first so will be ignored (move featureMap before "%s").' %
                        (item.nodeid, item._obj.func_code.co_varnames[idxfm-1]) )
                continue
            fmapped[item.nodeid] = []
            for idx in range(1, len(item._obj.func_code.co_consts)):
                requirement = item._obj.func_code.co_consts[idx]
                if not (type(requirement) == str and requirement.startswith(tuple(TestLog.REQUIREMENT_PREFIXES))):
                    break
                item.add_marker(requirement)
                fmapped[item.nodeid].append(requirement)


@pytest.fixture()
def common_lib(request):
    """
    Common Library functions accessed through framework
    """

    return dp_lib
