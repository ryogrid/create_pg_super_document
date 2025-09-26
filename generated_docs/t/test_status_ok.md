# test_status_ok

## Location
[src/test/regress/pg_regress.c:302-309](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L302-L309)

## Overview
Records a successful test result and prints formatted TAP output for passed tests in the PostgreSQL regression testing framework.

## Definition
static void test_status_ok(const char *testname, double runtime, bool parallel)

## Detailed Description
This function handles the processing and reporting of successful test results in the PostgreSQL regression test suite. It performs two key operations: incrementing the global success counter to track the number of passed tests, and generating properly formatted TAP-compliant output through the test_status_print function. This function serves as a high-level interface for reporting test successes, abstracting away the formatting details while maintaining consistent test result tracking.

The function is part of the pg_regress test driver's TAP output generation system, ensuring that successful test results are properly counted and reported in a format that is both human-readable and compatible with automated test harnesses.

## Parameters / Member Variables
- `testname`: Name of the test that passed
- `runtime`: Execution time of the test in milliseconds
- `parallel`: Boolean indicating whether the test was run in parallel (true) or sequentially (false)

## Dependencies
- Functions called/Symbols referenced:
  - [test_status_print](test_status_print.md)
  - success_count (global variable)
- Called from (representative examples):
  - Test execution functions in the regression test framework
  - [run_single_test](../r/run_single_test.md) (when a test passes)

## Notes and Other Information
- This function automatically increments the global success_count variable to maintain accurate statistics
- The actual formatting and output generation is delegated to test_status_print with the 'ok' parameter set to true
- Part of a paired system with test_status_failed for comprehensive test result reporting
- Maintains the global test statistics that are used for final test run summaries