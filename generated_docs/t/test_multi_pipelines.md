# test_multi_pipelines

## Location
[src/test/modules/libpq_pipeline/libpq_pipeline.c:469-613](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/libpq_pipeline/libpq_pipeline.c#L469-L613)

## Overview
Tests the execution and processing of multiple consecutive pipelines within a single pipeline mode session, validating proper synchronization and result handling.

## Definition

```c
struct timeval tv;
```
## Detailed Description
The  function validates the ability to queue and process multiple separate pipelines consecutively within a single pipeline mode session. It demonstrates that multiple pipeline segments can be queued with their respective sync points and then processed sequentially without exiting and re-entering pipeline mode between each segment.

The function performs the following test sequence:
1. Enters pipeline mode
2. Queues three separate pipeline segments, each containing:
   - A parameterized SELECT query using PQsendQueryParams()
   - A pipeline sync marker using PQpipelineSync() or PQsendPipelineSync()
3. Processes results from each pipeline segment in order:
   - Retrieves the SELECT query result and validates it's PGRES_TUPLES_OK
   - Verifies no extra results exist for the query
   - Attempts to exit pipeline mode (should fail while sync pending)
   - Retrieves the sync result and validates it's PGRES_PIPELINE_SYNC
4. After processing all pipelines, successfully exits pipeline mode
5. Validates the final pipeline status is PQ_PIPELINE_OFF

## Parameters / Member Variables
- : The database connection to execute multiple pipelines on

## Dependencies
- Functions called/Symbols referenced:
  - [PQenterPipelineMode](../P/PQenterPipelineMode.md)
  - [PQsendQueryParams](../P/PQsendQueryParams.md)
  - [PQpipelineSync](../P/PQpipelineSync.md)
  - [PQsendPipelineSync](../P/PQsendPipelineSync.md)
  - [PQgetResult](../P/PQgetResult.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - [PQresStatus](../P/PQresStatus.md)
  - [PQclear](../P/PQclear.md)
  - [PQexitPipelineMode](../P/PQexitPipelineMode.md)
  - [PQpipelineStatus](../P/PQpipelineStatus.md)
  - [PQerrorMessage](../P/PQerrorMessage.md)
  - PGRES_TUPLES_OK
  - PGRES_PIPELINE_SYNC
  - PQ_PIPELINE_OFF
  - INT4OID
  - fprintf
  - [pg_fatal](../p/pg_fatal.md)
- Called from (representative examples):
  - [main](../m/main.md)

## Notes and Other Information
- This is a static function within the libpq_pipeline test module
- Demonstrates advanced pipeline usage with multiple consecutive pipeline segments
- Tests both PQpipelineSync() and PQsendPipelineSync() functions
- Uses parameterized queries with INT4OID parameter type for testing
- Validates that pipeline mode cannot be exited while sync results are pending
- Shows proper result processing pattern for pipelined queries
- Tests state management across multiple pipeline boundaries
- Located in src/test/modules/libpq_pipeline/libpq_pipeline.c at lines 469-613
- Essential for validating complex pipeline workflows and state transitions

## Simplified Source

```c
static void test_multi_pipelines(PGconn *conn) {
    PGresult *res = NULL;
    const char *dummy_params[1] = {"1"};
    Oid dummy_param_oids[1] = {INT4OID};

    fprintf(stderr, "multi pipeline... ");

    if (PQenterPipelineMode(conn) != 1)
        pg_fatal("failed to enter pipeline mode: %s", PQerrorMessage(conn));

    // Pipeline 1: Send query and sync
    if (PQsendQueryParams(conn, "SELECT $1", 1, dummy_param_oids,
                          dummy_params, NULL, NULL, 0) != 1)
        pg_fatal("dispatching first SELECT failed: %s", PQerrorMessage(conn));
    if (PQpipelineSync(conn) != 1)
        pg_fatal("Pipeline sync failed: %s", PQerrorMessage(conn));

    // Pipeline 2: Send query and sync (using PQsendPipelineSync)
    if (PQsendQueryParams(conn, "SELECT $1", 1, dummy_param_oids,
                          dummy_params, NULL, NULL, 0) != 1)
        pg_fatal("dispatching second SELECT failed: %s", PQerrorMessage(conn));
    if (PQsendPipelineSync(conn) != 1)
        pg_fatal("Pipeline sync failed: %s", PQerrorMessage(conn));

    // Pipeline 3: Send query and sync
    if (PQsendQueryParams(conn, "SELECT $1", 1, dummy_param_oids,
                          dummy_params, NULL, NULL, 0) != 1)
        pg_fatal("dispatching third SELECT failed: %s", PQerrorMessage(conn));
    if (PQpipelineSync(conn) != 1)
        pg_fatal("pipeline sync failed: %s", PQerrorMessage(conn));

    // Process results from all three pipelines
    for (int pipeline = 1; pipeline <= 3; pipeline++) {
        // Get query result
        res = PQgetResult(conn);
        if (res == NULL)
            pg_fatal("PQgetResult returned null when there's a pipeline item: %s",
                     PQerrorMessage(conn));
        if (PQresultStatus(res) != PGRES_TUPLES_OK)
            pg_fatal("Unexpected result code %s from pipeline %d",
                     PQresStatus(PQresultStatus(res)), pipeline);
        PQclear(res);

        // Verify no extra results
        if (PQgetResult(conn) != NULL)
            pg_fatal("PQgetResult returned something extra after result");

        // Cannot exit pipeline mode before sync
        if (PQexitPipelineMode(conn) != 0)
            pg_fatal("exiting pipeline mode before sync succeeded incorrectly");

        // Get sync result
        res = PQgetResult(conn);
        if (res == NULL)
            pg_fatal("PQgetResult returned null when sync result expected: %s",
                     PQerrorMessage(conn));
        if (PQresultStatus(res) != PGRES_PIPELINE_SYNC)
            pg_fatal("Unexpected result code %s instead of sync result",
                     PQresStatus(PQresultStatus(res)));
        PQclear(res);
    }

    // Now we can exit pipeline mode
    if (PQexitPipelineMode(conn) != 1)
        pg_fatal("attempt to exit pipeline mode failed: %s", PQerrorMessage(conn));

    if (PQpipelineStatus(conn) != PQ_PIPELINE_OFF)
        pg_fatal("exiting pipeline mode didn't seem to work");

    fprintf(stderr, "ok\n");
}
```