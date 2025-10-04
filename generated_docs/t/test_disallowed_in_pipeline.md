# test_disallowed_in_pipeline

## Location
[src/test/modules/libpq_pipeline/libpq_pipeline.c:409-468](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/libpq_pipeline/libpq_pipeline.c#L409-L468)

## Overview
Tests error handling and validation for operations that are not allowed in PostgreSQL pipeline mode, ensuring proper error messages and mode transitions.

## Definition

```c
static void
test_disallowed_in_pipeline(PGconn *conn)
```
## Detailed Description
The  function validates that certain libpq operations are properly restricted when the connection is in pipeline mode, and that appropriate error messages are returned when these restrictions are violated. It also tests the pipeline mode state transitions and ensures that re-entering and exiting pipeline mode works correctly.

The function performs the following test sequence:
1. Verifies the connection is in blocking mode initially
2. Enters pipeline mode and confirms the mode change
3. Tests that PQexec() fails with the expected error message in pipeline mode
4. Tests that PQsendQuery() fails with the expected error message in pipeline mode
5. Verifies that re-entering pipeline mode is allowed (no-op)
6. Checks PQisBusy() behavior in idle pipeline mode
7. Exits pipeline mode and confirms the mode change
8. Tests that exiting pipeline mode when not in pipeline mode is a no-op
9. Verifies that PQexec() works again after exiting pipeline mode

## Parameters / Member Variables
- `*conn`: The database connection to test pipeline mode restrictions on
## Dependencies
- Functions called/Symbols referenced:
  - [PQisnonblocking](../P/PQisnonblocking.md)
  - [PQenterPipelineMode](../P/PQenterPipelineMode.md)
  - [PQpipelineStatus](../P/PQpipelineStatus.md)
  - PQ_PIPELINE_OFF
  - [PQexec](../P/PQexec.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - PGRES_FATAL_ERROR
  - PGRES_TUPLES_OK
  - [PQerrorMessage](../P/PQerrorMessage.md)
  - [PQsendQuery](../P/PQsendQuery.md)
  - [PQisBusy](../P/PQisBusy.md)
  - [PQexitPipelineMode](../P/PQexitPipelineMode.md)
  - strcmp
  - fprintf
  - [pg_fatal](../p/pg_fatal.md)
- Called from (representative examples):
  - [main](../m/main.md)

## Notes and Other Information
- This is a static function within the libpq_pipeline test module
- Tests critical error handling paths for pipeline mode restrictions
- Validates specific error message content to ensure proper user feedback
- Ensures pipeline mode state transitions work correctly
- Verifies that certain operations become available again after exiting pipeline mode
- Uses strcmp() to validate exact error message content
- Part of the comprehensive pipeline mode test suite
- Located in src/test/modules/libpq_pipeline/libpq_pipeline.c at lines 409-468
- Essential for ensuring pipeline mode safety and proper API usage

## Simplified Source

```c
static void test_disallowed_in_pipeline(PGconn *conn) {
    PGresult *res = NULL;

    fprintf(stderr, "test error cases... ");

    // Verify connection is in blocking mode
    if (PQisnonblocking(conn))
        pg_fatal("Expected blocking connection mode");

    // Enter pipeline mode
    if (PQenterPipelineMode(conn) != 1)
        pg_fatal("Unable to enter pipeline mode");

    if (PQpipelineStatus(conn) == PQ_PIPELINE_OFF)
        pg_fatal("Pipeline mode not activated properly");

    // Test: PQexec should fail in pipeline mode
    res = PQexec(conn, "SELECT 1");
    if (PQresultStatus(res) != PGRES_FATAL_ERROR)
        pg_fatal("PQexec should fail in pipeline mode but succeeded");
    // Verify expected error message for PQexec
    if (strcmp(PQerrorMessage(conn),
               "synchronous command execution functions are not allowed in pipeline mode\n") != 0)
        pg_fatal("did not get expected error message; got: \"%s\"", PQerrorMessage(conn));

    // Test: PQsendQuery should fail in pipeline mode
    if (PQsendQuery(conn, "SELECT 1") != 0)
        pg_fatal("PQsendQuery should fail in pipeline mode but succeeded");
    // Verify expected error message for PQsendQuery
    if (strcmp(PQerrorMessage(conn), "PQsendQuery not allowed in pipeline mode\n") != 0)
        pg_fatal("did not get expected error message; got: \"%s\"", PQerrorMessage(conn));

    // Test: Re-entering pipeline mode should be allowed (no-op)
    if (PQenterPipelineMode(conn) != 1)
        pg_fatal("re-entering pipeline mode should be a no-op but failed");

    // Test: PQisBusy should return 0 when idle in pipeline mode
    if (PQisBusy(conn) != 0)
        pg_fatal("PQisBusy should return 0 when idle in pipeline mode, returned 1");

    // Exit pipeline mode
    if (PQexitPipelineMode(conn) != 1)
        pg_fatal("couldn't exit idle empty pipeline mode");

    if (PQpipelineStatus(conn) != PQ_PIPELINE_OFF)
        pg_fatal("Pipeline mode not terminated properly");

    // Test: Exiting pipeline mode when not in pipeline mode should be no-op
    if (PQexitPipelineMode(conn) != 1)
        pg_fatal("pipeline mode exit when not in pipeline mode should succeed but failed");

    // Test: PQexec should work again after exiting pipeline mode
    res = PQexec(conn, "SELECT 1");
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
        pg_fatal("PQexec should succeed after exiting pipeline mode but failed with: %s",
                 PQerrorMessage(conn));

    fprintf(stderr, "ok\n");
}
```