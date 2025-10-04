# test_simple_pipeline

## Location
[src/test/modules/libpq_pipeline/libpq_pipeline.c:1490-1576](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/libpq_pipeline/libpq_pipeline.c#L1490-L1576)

## Overview
Tests the basic functionality of PostgreSQL pipeline mode by entering pipeline mode, sending a single query, synchronizing, and properly exiting pipeline mode.

## Definition
```c
static void test_simple_pipeline(PGconn *conn)
```

## Detailed Description
This function tests the fundamental pipeline mode operations in libpq. It demonstrates the complete lifecycle of pipeline mode usage:

1. **Pipeline Mode Entry**: Enters pipeline mode using `PQenterPipelineMode()`
2. **Query Dispatch**: Sends a parameterized SELECT query using `PQsendQueryParams()`
3. **Pipeline Sync**: Issues a pipeline synchronization point with `PQpipelineSync()`
4. **Result Processing**: Retrieves and validates the query result, expecting `PGRES_TUPLES_OK`
5. **Sync Result Processing**: Retrieves and validates the sync result, expecting `PGRES_PIPELINE_SYNC`
6. **Pipeline Mode Exit**: Properly exits pipeline mode using `PQexitPipelineMode()`

The test includes several validation checks to ensure pipeline mode behaves correctly:
- Prevents exiting pipeline mode while work is in progress
- Ensures proper result sequencing (query result followed by sync result)
- Validates pipeline status throughout the process
- Confirms clean exit from pipeline mode

## Parameters / Member Variables
- `conn`: PostgreSQL connection object (`PGconn *`) in blocking mode used for pipeline operations

## Dependencies
- Functions called/Symbols referenced:
  - [PQisnonblocking](../P/PQisnonblocking.md) - Check if connection is in non-blocking mode
  - [PQenterPipelineMode](../P/PQenterPipelineMode.md) - Enter pipeline mode
  - [PQsendQueryParams](../P/PQsendQueryParams.md) - Send parameterized query
  - [PQexitPipelineMode](../P/PQexitPipelineMode.md) - Exit pipeline mode
  - [PQpipelineSync](../P/PQpipelineSync.md) - Send pipeline synchronization
  - [PQgetResult](../P/PQgetResult.md) - Retrieve query results
  - [PQresultStatus](../P/PQresultStatus.md) - Get result status
  - [PQresStatus](../P/PQresStatus.md) - Get status string
  - [PQpipelineStatus](../P/PQpipelineStatus.md) - Get current pipeline status
  - [PQerrorMessage](../P/PQerrorMessage.md) - Get error message
  - [PQclear](../P/PQclear.md) - Free result memory
  - PGRES_TUPLES_OK - Expected result status constant
  - PGRES_PIPELINE_SYNC - Expected sync result status constant
  - PQ_PIPELINE_OFF - Pipeline mode off status constant
- Called from (representative examples):
  - [main](../m/main.md) - Main test driver function

## Notes and Other Information
- This is a test function specifically designed to validate basic pipeline mode functionality
- Requires a blocking connection (non-blocking mode will cause the test to fail)
- Uses a simple parameterized query (`SELECT $1` with integer parameter "1")
- Demonstrates proper error handling and validation throughout the pipeline lifecycle
- Part of the libpq_pipeline test module located in `src/test/modules/libpq_pipeline/`
- The test validates that pipeline mode cannot be exited prematurely (while work is in progress)
- Ensures that sync results are properly received and processed after query results

## Simplified Source

```c
static void test_simple_pipeline(PGconn *conn) {
    PGresult *res = NULL;
    const char *dummy_params[1] = {"1"};
    Oid dummy_param_oids[1] = {INT4OID};

    fprintf(stderr, "simple pipeline... ");

    // Verify blocking mode and enter pipeline mode
    if (PQisnonblocking(conn))
        pg_fatal("Expected blocking connection mode");
    if (PQenterPipelineMode(conn) != 1)
        pg_fatal("failed to enter pipeline mode: %s", PQerrorMessage(conn));

    // Send query and attempt early pipeline exit (should fail)
    if (PQsendQueryParams(conn, "SELECT $1", 1, dummy_param_oids,
                         dummy_params, NULL, NULL, 0) != 1)
        pg_fatal("dispatching SELECT failed: %s", PQerrorMessage(conn));

    if (PQexitPipelineMode(conn) != 0)
        pg_fatal("exiting pipeline mode with work in progress should fail, but succeeded");

    // Send pipeline sync and process results
    if (PQpipelineSync(conn) != 1)
        pg_fatal("pipeline sync failed: %s", PQerrorMessage(conn));

    // Get and validate query result
    res = PQgetResult(conn);
    if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
        pg_fatal("Unexpected result from first pipeline item");
    PQclear(res);

    // Ensure no extra results after query
    if (PQgetResult(conn) != NULL)
        pg_fatal("PQgetResult returned something extra after first query result.");

    // Get and validate sync result
    res = PQgetResult(conn);
    if (res == NULL || PQresultStatus(res) != PGRES_PIPELINE_SYNC)
        pg_fatal("Expected PGRES_PIPELINE_SYNC");
    PQclear(res);

    // Ensure no extra results after sync
    if (PQgetResult(conn) != NULL)
        pg_fatal("PQgetResult returned something extra after pipeline end");

    // Verify still in pipeline mode, then exit successfully
    if (PQpipelineStatus(conn) == PQ_PIPELINE_OFF)
        pg_fatal("Fell out of pipeline mode somehow");

    if (PQexitPipelineMode(conn) != 1)
        pg_fatal("attempt to exit pipeline mode failed when it should've succeeded");

    if (PQpipelineStatus(conn) != PQ_PIPELINE_OFF)
        pg_fatal("Exiting pipeline mode didn't seem to work");

    fprintf(stderr, "ok\n");
}
```