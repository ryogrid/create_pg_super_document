# print_test_list

## Location
[src/test/modules/libpq_pipeline/libpq_pipeline.c:2149-2165](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/libpq_pipeline/libpq_pipeline.c#L2149-L2165)

## Overview
Prints a list of available test names for the libpq pipeline testing utility, displaying all supported test cases to stdout.

## Definition

```c
static void
print_test_list(void)
```
## Detailed Description
The  function is a static helper function in the libpq_pipeline test utility that prints the names of all available test cases to stdout. This function is called when the user specifies "tests" as the test name argument, providing a way to discover what pipeline tests are available. Each test name is printed on a separate line, making it easy to parse or read. The function lists 12 different test cases that verify various aspects of libpq's pipeline execution functionality.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - printf (standard C library function)
- Called from (representative examples):
  - [main](../m/main.md) function in src/test/modules/libpq_pipeline/libpq_pipeline.c:2209 (when testname equals "tests")

## Notes and Other Information
- This function lists the following test cases:
  - cancel: Tests query cancellation in pipeline mode
  - disallowed_in_pipeline: Tests operations that are not allowed in pipeline mode
  - multi_pipelines: Tests multiple concurrent pipelines
  - nosync: Tests pipeline execution without synchronization
  - pipeline_abort: Tests pipeline abortion scenarios
  - pipeline_idle: Tests pipeline behavior when idle
  - pipelined_insert: Tests batch insert operations using pipelines
  - prepared: Tests prepared statements in pipeline mode
  - simple_pipeline: Tests basic pipeline functionality
  - singlerow: Tests single-row mode in pipelines
  - transaction: Tests transaction handling in pipeline mode
  - uniqviol: Tests unique constraint violations in pipeline mode
- The function is part of the PostgreSQL test suite for verifying libpq's pipeline execution capabilities
- Used for test discovery and documentation purposes in the test framework