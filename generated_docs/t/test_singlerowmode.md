# test_singlerowmode

## Location
[src/test/modules/libpq_pipeline/libpq_pipeline.c:1577-1772](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/libpq_pipeline/libpq_pipeline.c#L1577-L1772)

## Overview
Tests single-row mode and chunked row mode functionality within PostgreSQL pipeline mode, ensuring proper result handling and mode switching behavior.

## Definition
```c
static void test_singlerowmode(PGconn *conn)
```

## Detailed Description
This comprehensive test function validates single-row mode and chunked row mode operations within pipeline mode. The test consists of several distinct phases:

1. **Pipeline Setup with Single-Row Mode**: Enters pipeline mode and sends three `generate_series` queries, applying single-row mode to the first two queries only
2. **Result Processing**: Processes results, expecting `PGRES_SINGLE_TUPLE` for queries 0-1 and `PGRES_TUPLES_OK` for query 2
3. **Mode Reset Verification**: Tests that single-row mode is properly reset between queries by sending a query with single-row mode followed by a normal query
4. **Chunked Row Mode Testing**: Tests `PQsetChunkedRowsMode()` with a chunk size of 3, verifying that results are delivered in chunks of the specified size with a partial final chunk

The function validates:
- Proper single-row mode activation and result processing
- Correct result status transitions (SINGLE_TUPLE → TUPLES_OK)
- Mode isolation between queries
- Chunked row mode functionality with partial chunks
- Pipeline synchronization and proper pipeline exit

## Parameters / Member Variables
- `conn`: PostgreSQL connection object (`PGconn *`) used for pipeline operations and result retrieval

## Dependencies
- Functions called/Symbols referenced:
  - [PQenterPipelineMode](../P/PQenterPipelineMode.md) - Enter pipeline mode
  - [PQsendQueryParams](../P/PQsendQueryParams.md) - Send parameterized queries
  - [PQpipelineSync](../P/PQpipelineSync.md) - Send pipeline synchronization
  - [PQsetSingleRowMode](../P/PQsetSingleRowMode.md) - Enable single-row mode
  - [PQsetChunkedRowsMode](../P/PQsetChunkedRowsMode.md) - Enable chunked row mode
  - [PQgetResult](../P/PQgetResult.md) - Retrieve query results
  - [PQsendFlushRequest](../P/PQsendFlushRequest.md) - Send flush request
  - [PQexitPipelineMode](../P/PQexitPipelineMode.md) - Exit pipeline mode
  - [PQresultStatus](../P/PQresultStatus.md) - Get result status
  - [PQresStatus](../P/PQresStatus.md) - Get status string representation
  - [PQntuples](../P/PQntuples.md) - Get number of tuples in result
  - [PQgetvalue](../P/PQgetvalue.md) - Get specific field value
  - [PQclear](../P/PQclear.md) - Free result memory
  - [PQerrorMessage](../P/PQerrorMessage.md) - Get error message
  - PGRES_SINGLE_TUPLE - Single tuple result status
  - PGRES_TUPLES_OK - Normal tuples result status
  - PGRES_TUPLES_CHUNK - Chunked tuples result status
  - PGRES_PIPELINE_SYNC - Pipeline sync result status
  - ExecStatusType - [Result](../R/Result.md) status enumeration type
- Called from (representative examples):
  - [main](../m/main.md) - Main test driver function

## Notes and Other Information
- This is a comprehensive test function for advanced libpq result processing modes
- Tests three distinct query results using `generate_series(42, $1)` with parameters 44, 45, and 46
- Single-row mode is applied only to the first two queries, demonstrating selective mode application
- The test validates that single-row mode automatically resets between queries
- Chunked row mode test uses `generate_series(1, 5)` with chunk size 3, expecting two chunks (3 rows + 2 rows)
- Part of the libpq_pipeline test module located in `src/test/modules/libpq_pipeline/`
- Demonstrates proper error handling and result validation throughout different processing modes
- The test ensures that mode flags are properly isolated and do not affect subsequent queries
- Validates that pipeline mode can handle mixed result processing modes within a single pipeline

## Simplified Source

```c
static void test_singlerowmode(PGconn *conn) {
    PGresult *res;
    int i;
    bool pipeline_ended = false;

    if (PQenterPipelineMode(conn) != 1)
        pg_fatal("failed to enter pipeline mode");

    // Send three queries: generate_series(42, 44), (42, 45), (42, 46)
    for (i = 0; i < 3; i++) {
        char *param[1];
        param[0] = psprintf("%d", 44 + i);

        if (PQsendQueryParams(conn, "SELECT generate_series(42, $1)",
                             1, NULL, (const char **) param,
                             NULL, NULL, 0) != 1)
            pg_fatal("failed to send query");
        pfree(param[0]);
    }
    if (PQpipelineSync(conn) != 1)
        pg_fatal("pipeline sync failed");

    // Process results: single-row mode for first 2, normal mode for 3rd
    for (i = 0; !pipeline_ended; i++) {
        bool first = true;
        bool saw_ending_tuplesok;

        // Enable single row mode for first 2 queries only
        if (i < 2) {
            if (PQsetSingleRowMode(conn) != 1)
                pg_fatal("PQsetSingleRowMode() failed for i=%d", i);
        }

        // Process all results for this query
        saw_ending_tuplesok = false;
        while ((res = PQgetResult(conn)) != NULL) {
            ExecStatusType est = PQresultStatus(res);

            if (est == PGRES_PIPELINE_SYNC) {
                fprintf(stderr, "end of pipeline reached\n");
                pipeline_ended = true;
                PQclear(res);
                break;
            }

            // Validate expected result types
            if (first) {
                if (i <= 1 && est != PGRES_SINGLE_TUPLE)
                    pg_fatal("Expected PGRES_SINGLE_TUPLE for query %d", i);
                if (i >= 2 && est != PGRES_TUPLES_OK)
                    pg_fatal("Expected PGRES_TUPLES_OK for query %d", i);
                first = false;
            }

            // Process different result types
            switch (est) {
                case PGRES_TUPLES_OK:
                    saw_ending_tuplesok = true;
                    break;
                case PGRES_SINGLE_TUPLE:
                    // Single tuple processing
                    break;
                default:
                    pg_fatal("unexpected result status");
            }
            PQclear(res);
        }
        if (!pipeline_ended && !saw_ending_tuplesok)
            pg_fatal("didn't get expected terminating TUPLES_OK");
    }

    // Test single-row mode reset between queries
    if (PQsendQueryParams(conn, "SELECT generate_series(0, 0)",
                         0, NULL, NULL, NULL, NULL, 0) != 1)
        pg_fatal("failed to send query");
    if (PQsetSingleRowMode(conn) != 1)
        pg_fatal("PQsetSingleRowMode() failed");

    // Verify single-row mode works
    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_SINGLE_TUPLE)
        pg_fatal("Expected PGRES_SINGLE_TUPLE");
    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
        pg_fatal("Expected PGRES_TUPLES_OK");

    // Test chunked row mode
    if (PQsendQueryParams(conn, "SELECT generate_series(1, 5)",
                         0, NULL, NULL, NULL, NULL, 0) != 1)
        pg_fatal("failed to send query");
    if (PQsetChunkedRowsMode(conn, 3) != 1)
        pg_fatal("PQsetChunkedRowsMode() failed");

    // Expect 3 rows, then 2 rows, then empty result
    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_TUPLES_CHUNK || PQntuples(res) != 3)
        pg_fatal("Expected 3-row chunk");
    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_TUPLES_CHUNK || PQntuples(res) != 2)
        pg_fatal("Expected 2-row chunk");
    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) != 0)
        pg_fatal("Expected empty final result");

    if (PQexitPipelineMode(conn) != 1)
        pg_fatal("failed to end pipeline mode");

    fprintf(stderr, "ok\n");
}
```