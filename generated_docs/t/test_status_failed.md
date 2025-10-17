# test_status_failed

## Location
[src/test/regress/pg_regress.c:310-329](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L310-L329)

## Overview
Records a failed test result, maintains a buffer of failed test names for summary reporting, and prints formatted TAP output for failed tests.

## Definition
static void test_status_failed(const char *testname, double runtime, bool parallel)

## Detailed Description
This function handles the comprehensive processing of failed test results in the PostgreSQL regression testing framework. Unlike test_status_ok, this function performs additional bookkeeping by maintaining a StringInfo buffer (failed_tests) that accumulates the names of all failed tests for summary reporting at the end of the test run. This buffer is formatted as a comma-separated list of test names and is later used with diagnostic output to ensure failed tests are prominently displayed even under test harnesses that might suppress normal output.

The function increments the global fail_count and delegates the actual TAP output formatting to test_status_print with the 'ok' parameter set to false. This design ensures that failed test information is both immediately reported and preserved for end-of-run summaries.

## Parameters / Member Variables
- `testname`: Name of the test that failed
- `runtime`: Execution time of the test in milliseconds  
- `parallel`: Boolean indicating whether the test was run in parallel (true) or sequentially (false)

## Dependencies
- Functions called/Symbols referenced:
  - [test_status_print](test_status_print.md)
  - [makeStringInfo](../m/makeStringInfo.md)
  - [appendStringInfoChar](../a/appendStringInfoChar.md)
  - [appendStringInfo](../a/appendStringInfo.md)
  - fail_count (global variable)
  - failed_tests (global StringInfo variable)
- Called from (representative examples):
  - Test execution functions in the regression test framework
  - [run_single_test](../r/run_single_test.md) (when a test fails)

## Notes and Other Information
- Maintains a global buffer (failed_tests) of all failed test names for summary reporting
- The failed test buffer is formatted as a comma-separated list, initialized on first failure
- Uses StringInfo for efficient string building when accumulating failed test names
- The collected failed test information is typically output at the end using diag() to ensure visibility under test harnesses
- Part of a paired system with test_status_ok, but includes additional failure tracking mechanisms

## Simplified Source

```c
static void test_status_failed(const char *testname, double runtime, bool parallel) {
    // Initialize failed tests buffer on first failure
    if (!failed_tests) {
        failed_tests = makeStringInfo();
    } else {
        // Add comma separator for subsequent failures
        appendStringInfoChar(failed_tests, ',');
    }

    // Add test name to failed tests list
    appendStringInfo(failed_tests, " %s", testname);

    // Increment global failure counter
    fail_count++;

    // Print TAP-formatted failure output
    test_status_print(false, testname, runtime, parallel);
}
```