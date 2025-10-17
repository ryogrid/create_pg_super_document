# report_result

## Location
[src/test/modules/test_escape/test_escape.c:119-155](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_escape/test_escape.c#L119-L155)

## Overview
A test reporting function that outputs test results in a structured format, managing test counters and controlling output verbosity based on configuration settings.

## Definition
```c
static void
report_result(pe_test_config *tc,
              bool success,
              const char *testname,
              const char *details,
              const char *subname,
              const char *resultdesc)
```

## Detailed Description
The `report_result` function is a central component of PostgreSQL's test escape module that handles test result reporting. It follows the TAP (Test Anything Protocol) format for output, producing standardized "ok" or "not ok" messages with test numbers. The function manages test counting, failure tracking, and output verbosity control.

The function increments the test counter for each call and conditionally prints test details and results based on the verbosity level configured in the test configuration. For successful tests, details and results may be suppressed based on verbosity settings, while failed tests are always reported and increment the failure counter.

## Parameters / Member Variables
- `tc`: Pointer to pe_test_config structure containing test configuration and counters
- `success`: Boolean indicating whether the test passed (true) or failed (false)
- `testname`: String identifier for the test category or function being tested
- `details`: Detailed information about the test (printed based on verbosity settings)
- `subname`: Sub-test or specific test case identifier within the test category
- `resultdesc`: Description of the test result or what was being verified

## Dependencies
- Functions called/Symbols referenced:
  - [pe_test_config](../p/pe_test_config.md) (struct type)
  - printf (standard library function)
- Called from (representative examples):
  - [test_gb18030_page_multiple](../t/test_gb18030_page_multiple.md)
  - [test_gb18030_json](../t/test_gb18030_json.md)
  - [test_psql_parse](../t/test_psql_parse.md)
  - [test_one_vector_escape](../t/test_one_vector_escape.md)

## Notes and Other Information
- This is a static function, accessible only within the test_escape.c file
- Follows TAP (Test Anything Protocol) output format with "ok" and "not ok" messages
- Automatically manages test numbering by incrementing tc->test_count
- Tracks failures by incrementing tc->failure_count for unsuccessful tests
- Verbosity control: verbosity <= 0 suppresses details for successful tests, verbosity < 0 suppresses result output for successful tests
- Part of PostgreSQL's testing infrastructure, not core database functionality

## Simplified Source

```c
static void
report_result(pe_test_config *tc, bool success, const char *testname,
              const char *details, const char *subname, const char *resultdesc)
{
    int test_id = ++tc->test_count;
    bool print_details = true;
    bool print_result = true;

    // Handle failure tracking and verbosity settings
    if (success) {
        if (tc->verbosity <= 0)
            print_details = false;
        if (tc->verbosity < 0)
            print_result = false;
    } else {
        tc->failure_count++;
    }

    // Print test details if appropriate
    if (print_details)
        printf("%s", details);

    // Print TAP-format result line if appropriate
    if (print_result)
        printf("%s %d - %s: %s: %s\n",
               success ? "ok" : "not ok",
               test_id, testname, subname, resultdesc);
}
```