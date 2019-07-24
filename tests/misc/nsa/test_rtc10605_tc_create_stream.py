#!/usr/bin/env python
"""
Test cases to create and validate streams in RIO
"""

# NOT AS PART OF THIS REVIEW

# TODO:to be refactored, should include a 'Stream' model just like a 'Recording model', constants to be cleaned up, and
# appropriate exceptions caught

import logging
import sys
import time

import pytest
import requests

from helpers.vmr.nsa import nsa_helper

LOGGER = logging.getLogger("test_logger")
STREAM_ID = ""
STREAM_ID_ARG = 'StreamID'
STREAM_NAME = ""
STREAM_NAME_ARG = 'StreamName'
STREAM_NAME_PREFIX = 'test_stream_id-'


# TODO:to be refactored
@pytest.mark.skip(reason="This test case has not been reviewed by PO yet")
def test_create_stream():
    """
    Test Creating of stream with given data
    """
    LOGGER.info("Starting the test: test_create_stream")
    STREAM_ID = str(int(time.time()))
    STREAM_NAME = STREAM_NAME_PREFIX + STREAM_ID

    try:
        response = nsa_helper.create_stream(STREAM_ID, STREAM_NAME)
    except Exception:
        LOGGER.info("exception while creating stream and test_create_stream has failed")
        exception_info = sys.exc_info()
        LOGGER.debug("Exception info-" + "type-" + str(exception_info[0]) + " message-" + str(exception_info[1]))
        assert False

    if response.status_code == requests.codes.no_content:
        LOGGER.info("create stream request has passed with a response status code " + str(response.status_code))
    else:
        LOGGER.error("ERROR: test_create_stream has failed. create stream request has failed with a response status code " + str(response.status_code))
        assert False

    try:
        is_valid_stream = nsa_helper.validate_stream(STREAM_ID, STREAM_NAME)
    except Exception:
        LOGGER.info("exception while validating stream and test_create_stream has failed")
        exception_info = sys.exc_info()
        LOGGER.debug("Exception info-" + "type-" + str(exception_info[0]) + " message-" + str(exception_info[1]))
        assert False

    if is_valid_stream:
        LOGGER.info("test_create_stream has passed")
    else:
        LOGGER.error("ERROR: test_create_stream has failed")
        assert False

    assert True
