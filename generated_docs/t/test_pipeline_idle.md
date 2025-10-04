# test_pipeline_idle

## Location
[src/test/modules/libpq_pipeline/libpq_pipeline.c:1423-1489](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/libpq_pipeline/libpq_pipeline.c#L1423-L1489)

## Overview
Tests PostgreSQL pipeline mode behavior in idle states, including restrictions on exiting pipeline mode and proper handling of notices during pipeline operations.

## Definition

```c
static void
test_pipeline_idle(PGconn *conn)
```
## Detailed Description
This function tests specific edge cases and state management aspects of PostgreSQL's pipeline mode, focusing on "idle" state behavior. The test performs several critical validations:

1. **Pipeline Exit Restrictions**: Tests that pipeline mode cannot be exited when there are pending operations or unflushed commands in the pipeline
2. **State Transition Validation**: Ensures proper state management by attempting to exit pipeline mode at inappropriate times and verifying expected error messages
3. **Notice Handling**: Sets up a notice processor to verify that notices are properly handled during pipeline operations
4. **Advisory Lock Testing**: Tests pipeline behavior with advisory lock operations that may generate warnings

The function demonstrates that PostgreSQL enforces proper pipeline state management by preventing premature exits from pipeline mode and ensuring all queued operations are properly handled before allowing state transitions.

## Parameters / Member Variables
- `*conn`: PostgreSQL connection handle for pipeline operations
## Dependencies
- Functions called/Symbols referenced:
  - [PQsetNoticeProcessor](../P/PQsetNoticeProcessor.md) (notice handler setup)
  - [PQenterPipelineMode](../P/PQenterPipelineMode.md)/PQexitPipelineMode (pipeline mode control)
  - [PQsendQueryParams](../P/PQsendQueryParams.md) (sending parameterized queries)
  - [PQsendFlushRequest](../P/PQsendFlushRequest.md) (forcing output buffer flush)
  - [PQgetResult](../P/PQgetResult.md) (retrieving results)
  - [PQresultStatus](../P/PQresultStatus.md)/PQresStatus (result status checking)
  - [PQerrorMessage](../P/PQerrorMessage.md) (error message retrieval)
  - [notice_processor](../n/notice_processor.md) (callback for handling notices)
  - PGRES_TUPLES_OK (expected result status)
- Called from (representative examples):
  - [main](../m/main.md) (at src/test/modules/libpq_pipeline/libpq_pipeline.c:2264)

## Notes and Other Information
- Tests error conditions that should prevent pipeline mode exit
- Validates proper error message content for invalid state transitions
- Includes testing of advisory lock functions which may generate PostgreSQL notices
- Ensures notice handling works correctly within pipeline contexts
- Critical for verifying pipeline state machine robustness and error handling
- Part of the libpq_pipeline test suite ensuring proper pipeline mode state management

## Simplified Source

```c
static void test_pipeline_idle(PGconn *conn) {
    PGresult *res;
    int n_notices = 0;

    fprintf(stderr, "\npipeline idle...\n");

    PQsetNoticeProcessor(conn, notice_processor, &n_notices);

    // Test 1: Cannot exit pipeline mode with pending operations
    if (PQenterPipelineMode(conn) != 1)
        pg_fatal("failed to enter pipeline mode: %s", PQerrorMessage(conn));

    if (PQsendQueryParams(conn, "SELECT 1", 0, NULL, NULL, NULL, NULL, 0) != 1)
        pg_fatal("failed to send query: %s", PQerrorMessage(conn));
    PQsendFlushRequest(conn);

    // Process first result
    res = PQgetResult(conn);
    if (res == NULL)
        pg_fatal("PQgetResult returned null when there's a pipeline item: %s",
                 PQerrorMessage(conn));
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
        pg_fatal("unexpected result code %s from first pipeline item",
                 PQresStatus(PQresultStatus(res)));
    PQclear(res);
    res = PQgetResult(conn);
    if (res != NULL)
        pg_fatal("did not receive terminating NULL");

    // Send second query but don't flush/process yet
    if (PQsendQueryParams(conn, "SELECT 2", 0, NULL, NULL, NULL, NULL, 0) != 1)
        pg_fatal("failed to send query: %s", PQerrorMessage(conn));

    // Attempt to exit pipeline mode should fail
    if (PQexitPipelineMode(conn) == 1)
        pg_fatal("exiting pipeline succeeded when it shouldn't");
    if (strncmp(PQerrorMessage(conn), "cannot exit pipeline mode",
                strlen("cannot exit pipeline mode")) != 0)
        pg_fatal("did not get expected error; got: %s", PQerrorMessage(conn));

    // Now flush and process second query
    PQsendFlushRequest(conn);
    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
        pg_fatal("unexpected result code %s from second pipeline item",
                 PQresStatus(PQresultStatus(res)));
    PQclear(res);
    res = PQgetResult(conn);
    if (res != NULL)
        pg_fatal("did not receive terminating NULL");

    // Now exit should succeed
    if (PQexitPipelineMode(conn) != 1)
        pg_fatal("exiting pipeline failed: %s", PQerrorMessage(conn));

    if (n_notices > 0)
        pg_fatal("got %d notice(s)", n_notices);
    fprintf(stderr, "ok - 1\n");

    // Test 2: Advisory lock with potential warning
    if (PQenterPipelineMode(conn) != 1)
        pg_fatal("entering pipeline mode failed: %s", PQerrorMessage(conn));

    if (PQsendQueryParams(conn, "SELECT pg_catalog.pg_advisory_unlock(1,1)",
                          0, NULL, NULL, NULL, NULL, 0) != 1)
        pg_fatal("failed to send query: %s", PQerrorMessage(conn));
    PQsendFlushRequest(conn);

    res = PQgetResult(conn);
    if (res == NULL)
        pg_fatal("unexpected NULL result received");
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
        pg_fatal("unexpected result code %s", PQresStatus(PQresultStatus(res)));

    if (PQexitPipelineMode(conn) != 1)
        pg_fatal("failed to exit pipeline mode: %s", PQerrorMessage(conn));

    fprintf(stderr, "ok - 2\n");
}
```