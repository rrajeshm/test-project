# Tests
This is the main folder which contains all the python test cases

----
# test lines added by Ram Rajesh

### File structure
* /`<suite>`
This folder contains the test suites for the test cases
    * Every test suite is a folder
    * The name of the folder is the name of the test suite
    * You can select the list of test suites to be run by mentioning this folder / test suite name
    * Currently there are three suites - basic, sanity and edge
    * Each of these suites can in turn contain one or more directories

* conftest.py
This files specifies the custom configuration options for py.test


----

### File structure
* /`<suite>`/`<dir>`/test_*
    * `<dir>` organizes the test cases within a test suite.
    * The tests are currently organized as  `recording`, `playback` and `archival`.
    * Every file in the folder starting with `test_` will be considered as a test case file.
    * Every function in the test case file starting with `test_` with be considered as a test case and will be run if the appropriate suite is selected