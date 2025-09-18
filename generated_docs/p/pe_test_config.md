# pe_test_config

## Location
src/test/modules/test_escape/test_escape.c: 24 - 33

## Overview
A configuration structure that manages test execution parameters and state for the PostgreSQL escape function test module.

## Definition


## Detailed Description
The  structure serves as the central configuration and state management object for the test_escape module. It encapsulates both test execution parameters (verbosity level, connection information) and runtime state (test counts, failure tracking). This structure is passed to various test functions to maintain consistent configuration and accumulate test results across different test scenarios.

## Parameters / Member Variables
- : Controls the level of output detail during test execution
- : Boolean flag to force execution of tests that might not be supported in current environment
- : Connection string used to establish database connection for tests
- : Active PostgreSQL connection handle used throughout test execution
- : Running count of total tests executed
- : Running count of tests that have failed

## Dependencies
- Functions called/Symbols referenced:
  - (No direct references from this structure)
- Called from (representative examples):
  - report_result
  - test_gb18030_page_multiple
  - test_gb18030_json
  - test_psql_parse
  - test_one_vector_escape
  - test_one_vector
  - main

## Notes and Other Information
This structure is fundamental to the test_escape module's architecture, providing a consistent interface for test configuration and result tracking. It enables the module to maintain state across multiple test functions while providing flexible configuration options for different testing scenarios.