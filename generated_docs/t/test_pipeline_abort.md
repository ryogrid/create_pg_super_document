# test_pipeline_abort

## Location
[src/test/modules/libpq_pipeline/libpq_pipeline.c:706-994](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/libpq_pipeline/libpq_pipeline.c#L706-L994)

## Overview
Tests PostgreSQL pipeline mode error handling by verifying that when an operation in a pipeline fails, the rest of the pipeline is properly aborted and subsequent commands receive PGRES_PIPELINE_ABORTED status.

## Definition

```c
enum for test_pipelined_insert */
enum PipelineInsertStep
{
	BI_BEGIN_TX,
	BI_DROP_TABLE,
	BI_CREATE_TABLE,
	BI_PREPARE,
	BI_INSERT_ROWS,
	BI_COMMIT_TX,
	BI_SYNC,
	BI_DONE,
};
```
## Detailed Description
This function comprehensively tests PostgreSQL's pipeline mode abort behavior by intentionally creating error conditions within pipelines and verifying the proper handling of aborted operations. The test creates multiple pipelines where:

1. **First Pipeline**: Contains a valid INSERT, an intentional error (calling non-existent function), and another INSERT that should be aborted
2. **Second Pipeline**: Contains a single INSERT that should execute normally after the first pipeline's error is handled
3. **Additional Tests**: Tests multiple commands in a single query string (which should error) and single-row mode with division-by-zero error

The function verifies that aborted pipeline operations return PGRES_PIPELINE_ABORTED status codes, pipeline sync operations work correctly, and that the pipeline mode state transitions are handled properly throughout the error recovery process.

## Parameters / Member Variables
- : PostgreSQL connection handle used for executing pipeline operations

## Dependencies
- Functions called/Symbols referenced:
  - [PQexec](../P/PQexec.md) (multiple calls for setup and verification)
  - [PQenterPipelineMode](../P/PQenterPipelineMode.md)/PQexitPipelineMode (pipeline mode control)
  - [PQsendQueryParams](../P/PQsendQueryParams.md) (sending parameterized queries)
  - [PQpipelineSync](../P/PQpipelineSync.md) (pipeline synchronization)
  - [PQgetResult](../P/PQgetResult.md) (retrieving results)
  - [PQpipelineStatus](../P/PQpipelineStatus.md) (checking pipeline state)
  - [PQsetSingleRowMode](../P/PQsetSingleRowMode.md) (enabling single-row mode)
  - [PQresultStatus](../P/PQresultStatus.md)/PQresStatus (result status checking)
  - [PQresultErrorField](../P/PQresultErrorField.md) (error field extraction)
  - Various PGRES_* constants (result status codes)
- Called from (representative examples):
  - [main](../m/main.md) (at src/test/modules/libpq_pipeline/libpq_pipeline.c:2262)

## Notes and Other Information
- Intentionally avoids using transactions to wrap pipelines to observe individual statement effects
- Tests both successful and error scenarios within the same pipeline sequence
- Verifies that only the INSERT with value '3' (from the second pipeline) remains in the database after completion
- Includes comprehensive error checking and state validation throughout the pipeline execution
- Tests advanced features like single-row mode combined with error handling
- Part of the libpq_pipeline test module for validating PostgreSQL client library pipeline functionality

## Simplified Source

```c
static void test_pipeline_abort(PGconn *conn) {
    PGresult *res = NULL;
    const char *dummy_params[1] = {"1"};
    Oid dummy_param_oids[1] = {INT4OID};

    fprintf(stderr, "aborted pipeline... ");

    // Setup: drop and create table
    res = PQexec(conn, drop_table_sql);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        pg_fatal("dispatching DROP TABLE failed: %s", PQerrorMessage(conn));

    res = PQexec(conn, create_table_sql);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        pg_fatal("dispatching CREATE TABLE failed: %s", PQerrorMessage(conn));

    if (PQenterPipelineMode(conn) != 1)
        pg_fatal("failed to enter pipeline mode: %s", PQerrorMessage(conn));

    // Pipeline 1: INSERT, ERROR, INSERT (should abort after error)
    dummy_params[0] = "1";
    if (PQsendQueryParams(conn, insert_sql, 1, dummy_param_oids,
                          dummy_params, NULL, NULL, 0) != 1)
        pg_fatal("dispatching first insert failed: %s", PQerrorMessage(conn));

    // Send intentional error query
    if (PQsendQueryParams(conn, "SELECT no_such_function($1)",
                          1, dummy_param_oids, dummy_params,
                          NULL, NULL, 0) != 1)
        pg_fatal("dispatching error select failed: %s", PQerrorMessage(conn));

    dummy_params[0] = "2";
    if (PQsendQueryParams(conn, insert_sql, 1, dummy_param_oids,
                          dummy_params, NULL, NULL, 0) != 1)
        pg_fatal("dispatching second insert failed: %s", PQerrorMessage(conn));

    if (PQpipelineSync(conn) != 1)
        pg_fatal("pipeline sync failed: %s", PQerrorMessage(conn));

    // Pipeline 2: Single INSERT (should succeed)
    dummy_params[0] = "3";
    if (PQsendQueryParams(conn, insert_sql, 1, dummy_param_oids,
                          dummy_params, NULL, NULL, 0) != 1)
        pg_fatal("dispatching second-pipeline insert failed: %s", PQerrorMessage(conn));

    if (PQpipelineSync(conn) != 1)
        pg_fatal("pipeline sync failed: %s", PQerrorMessage(conn));

    // Process results: First INSERT should succeed
    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        pg_fatal("First insert should succeed");
    PQclear(res);
    PQgetResult(conn); // consume NULL

    // Second query should error
    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_FATAL_ERROR)
        pg_fatal("Expected PGRES_FATAL_ERROR for no_such_function");
    PQclear(res);
    PQgetResult(conn); // consume NULL

    // Pipeline should be aborted
    if (PQpipelineStatus(conn) != PQ_PIPELINE_ABORTED)
        pg_fatal("pipeline should be flagged as aborted");

    // Third query should be aborted
    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_PIPELINE_ABORTED)
        pg_fatal("Expected PGRES_PIPELINE_ABORTED for third query");
    PQclear(res);
    PQgetResult(conn); // consume NULL

    // Get pipeline sync (clears abort flag)
    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_PIPELINE_SYNC)
        pg_fatal("Expected PGRES_PIPELINE_SYNC");
    PQclear(res);

    // Second pipeline should succeed
    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        pg_fatal("Second pipeline insert should succeed");
    PQclear(res);
    PQgetResult(conn); // consume NULL

    res = PQgetResult(conn);
    if (PQresultStatus(res) != PGRES_PIPELINE_SYNC)
        pg_fatal("Expected second pipeline sync");
    PQclear(res);

    // Additional tests (multiple commands, single-row mode) - simplified
    // ... (details omitted for brevity)

    if (PQexitPipelineMode(conn) != 1)
        pg_fatal("failed to exit pipeline mode");

    // Verify only value "3" remains in table (from second pipeline)
    res = PQexec(conn, "SELECT itemno FROM pq_pipeline_demo");
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
        pg_fatal("Expected tuples result");
    if (PQntuples(res) != 1 || strcmp(PQgetvalue(res, 0, 0), "3") != 0)
        pg_fatal("Expected only insert with value 3");
    PQclear(res);

    fprintf(stderr, "ok\n");
}
```