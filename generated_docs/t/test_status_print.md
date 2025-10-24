# test_status_print

## Location
[src/test/regress/pg_regress.c:279-301](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L279-L301)

## Overview
Formats and prints TAP-compliant test status output with aligned test names and runtimes for both sequential and parallel test execution.

## Definition
static void test_status_print(bool ok, const char *testname, double runtime, bool parallel)

## Detailed Description
This function is responsible for generating TAP (Test Anything Protocol) compliant output for individual test results in the PostgreSQL regression testing framework. It formats the output to ensure human readability while maintaining TAP compliance. The function handles both successful and failed tests, and distinguishes between tests run in parallel versus sequentially through visual indicators.

The output format includes:
- Test status ("ok" or "not ok")
- Test number (padded to 5 characters)
- Parallel/sequential indicator ('+' for parallel, '-' for sequential)
- Test name (padded for alignment)
- Runtime in milliseconds

The formatting ensures vertical alignment of test names and runtimes across all test outputs, making it easier to read the results while still being parseable by TAP harnesses.

## Parameters / Member Variables
- : Boolean indicating whether the test passed (true) or failed (false)
- : Name of the test being reported
- : Execution time of the test in milliseconds
- : Boolean indicating if the test was run in parallel (true) or sequentially (false)

## Dependencies
- Functions called/Symbols referenced:
  - [emit_tap_output](../e/emit_tap_output.md)
  - fail_count (global variable)
  - success_count (global variable)
  - TEST_STATUS (TAPtype enum value)
  - TESTNAME_WIDTH (macro)
- Called from (representative examples):
  - [test_status_ok](test_status_ok.md)
  - [test_status_failed](test_status_failed.md)

## Notes and Other Information
- The function uses a specific formatting strategy where test numbers are padded to 5 characters (supporting up to 9999 tests)
- Visual indicators distinguish parallel tests ('+') from sequential tests ('-')
- When a test fails, "not " is prepended to the output, and successful tests are indented with spaces to maintain alignment
- The output format was designed to be compatible with both human readers and automated TAP parsers, particularly the meson TAP parser which consumes leading whitespace

## Simplified Source

```c
static void test_status_print(bool ok, const char *testname, double runtime, bool parallel) {
    int testnumber = fail_count + success_count;

    // Format TAP output with aligned columns:
    // - "ok"/"not ok" status
    // - Test number (padded to 5 chars)
    // - '+' for parallel, '-' for sequential
    // - Test name (padded for alignment)
    // - Runtime in milliseconds
    emit_tap_output(TEST_STATUS, "%sok %-5i%*s %c %-*s %8.0f ms",
                    (ok ? "" : "not "),
                    testnumber,
                    (ok ? (int) strlen("not ") : 0), "",  // Alignment spacing
                    (parallel ? '+' : '-'),
                    TESTNAME_WIDTH, testname,
                    runtime);
}
```