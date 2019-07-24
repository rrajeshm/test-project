#!/usr/bin/env python

"""
This is the point of entry to exercise the test suites associated with e2e data plane automation.
"""
import argparse
import sys
import os

import pytest

import helpers.setup as test_setup
from helpers.constants import PytestOptions, TestDirectory

# Enable console logging
test_setup.enable_logging()

# Identify Test types
test_dirs = [x for x in os.listdir(TestDirectory.TEST_DIR) 
             if os.path.isdir(os.path.join(TestDirectory.TEST_DIR, x)) and x[0].isalpha()]

def initialize_pytest_args():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-k", default=None, help="Run tests matching a specific string")
    arg_parser.add_argument("--pvt", action="store_true", default=False, help="Run tests matching a specific "
                                                                              "string for a private copy stream")
    arg_parser.add_argument("-m", default=None, help="Run tests with a specific mark - playback, recording, etc.")
    arg_parser.add_argument('--skip', default=None, nargs='*', metavar=('streamID', 'markerName'), action='append',
                      help="Skip test based on marker for specific stream. Example --skip <streamID> <markerName1> <markerName2>")
    arg_parser.add_argument("-t", default=None, choices=test_dirs, help="Run one or more specific tests")   
    arg_parser.add_argument("-d", default=None, help="Run destructive tests matching a specific string")
    arg_parser.add_argument("--dist", default=PytestOptions.XDIST_NO, choices=(PytestOptions.XDIST_EACH,
                                                                                 PytestOptions.XDIST_LOAD,
                                                                                 PytestOptions.XDIST_NO),
                              help="Control parallel execution of test cases. Default is '{0}'."
                              .format(PytestOptions.XDIST_NO))
    arg_parser.add_argument("-l", "--lab", metavar="labName", default=None, 
                      help="Run tests on a specific Lab configuration directory, found under directory: conf/")
    return arg_parser

if __name__ == "__main__":
    arg_parser = initialize_pytest_args()    
    pytest_args = test_setup.construct_pytest_args(arg_parser.parse_args(), arg_parser)

    test_run_result = pytest.main(pytest_args)  # Run the tests and pass the results of the test back to the caller

    sys.exit(test_run_result)

