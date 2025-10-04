# test_psql_parse

## Location
[src/test/modules/test_escape/test_escape.c:580-636](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_escape/test_escape.c#L580-L636)

## Overview
A test function that verifies that psql parses a given input as a single SQL statement to ensure escape functions effectively protect against SQL injection by preventing statement smuggling.

## Definition

```c
static void
test_psql_parse(pe_test_config *tc, PQExpBuffer testname,
				PQExpBuffer input_buf, PQExpBuffer details)
```
## Detailed Description
This function validates that the psql parser interprets the provided input buffer as a single SQL statement. It uses the psql scanner to parse the input and ensures that only one statement is detected. This verification is critical for testing escape functions because if an input can be parsed as multiple statements, it indicates that the escape function failed to prevent SQL injection through statement smuggling. The function scans the input using PostgreSQL's psql scanner, tracks the number of statements found, and reports whether the test passes or fails based on whether exactly one complete statement was parsed.

## Parameters / Member Variables
- `*tc`: Test configuration structure containing connection and test parameters
- `testname`: PQExpBuffer containing the name of the current test being executed
- `input_buf`: PQExpBuffer containing the input SQL text to be parsed and validated
- `details`: PQExpBuffer for accumulating detailed test output and diagnostic information
## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - psql_scan_create
  - psql_scan_setup
  - psql_scan
  - psql_scan_destroy
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [destroyPQExpBuffer](../d/destroyPQExpBuffer.md)
  - [PQclientEncoding](../P/PQclientEncoding.md)
  - scan_res_s
  - [escapify](../e/escapify.md)
  - [report_result](../r/report_result.md)
- Types referenced:
  - [pe_test_config](../p/pe_test_config.md)
  - [PsqlScanState](../P/PsqlScanState.md)
  - PsqlScanResult
  - promptStatus_t
  - PROMPT_READY
  - PSCAN_INCOMPLETE
  - PSCAN_EOL
- Called from (representative examples):
  - [test_one_vector_escape](test_one_vector_escape.md)

## Notes and Other Information
- This is a test module function located in 
- The function hardcodes standard conforming strings mode (TODO comment suggests testing without this as well)
- Test fails if more than one statement is detected or if parsing doesn't end in the expected PSCAN_EOL state
- Critical for security testing as it validates that escape functions prevent SQL injection through statement splitting
- Uses PostgreSQL's internal psql scanner to perform the parsing validation
- Provides detailed diagnostic output showing scan results, prompt status, and query buffer contents for each parsing iteration
- The test is specifically designed to catch cases where malicious input could be interpreted as multiple SQL statements

## Simplified Source
```c
static void
test_psql_parse(pe_test_config *tc, PQExpBuffer testname,
                PQExpBuffer input_buf, PQExpBuffer details)
{
    PsqlScanState scan_state;
    PsqlScanResult scan_result;
    PQExpBuffer query_buf;
    promptStatus_t prompt_status = PROMPT_READY;
    int matches = 0;

    query_buf = createPQExpBuffer();
    scan_state = psql_scan_create(&test_scan_callbacks);

    /* Setup scanner with standard conforming strings */
    psql_scan_setup(scan_state, input_buf->data, input_buf->len,
                    PQclientEncoding(tc->conn), 1);

    /* Scan input and count statements */
    do
    {
        resetPQExpBuffer(query_buf);
        scan_result = psql_scan(scan_state, query_buf, &prompt_status);

        /* Log scan details for debugging */
        appendPQExpBuffer(details,
                         "#\t\t %d: scan_result: %s prompt: %u, query_buf: ",
                         matches, scan_res_s(scan_result), prompt_status);
        escapify(details, query_buf->data, query_buf->len);
        appendPQExpBuffer(details, "\n");

        matches++;
    }
    while (scan_result != PSCAN_INCOMPLETE && scan_result != PSCAN_EOL);

    /* Cleanup */
    psql_scan_destroy(scan_state);
    destroyPQExpBuffer(query_buf);

    /* Test passes if exactly one statement and proper end state */
    bool test_fails = matches > 1 || scan_result != PSCAN_EOL;

    const char *resdesc;
    if (matches > 1)
        resdesc = "more than one match";
    else if (scan_result != PSCAN_EOL)
        resdesc = "unexpected end state";
    else
        resdesc = "ok";

    report_result(tc, !test_fails, testname->data, details->data,
                  "psql parse", resdesc);
}
```