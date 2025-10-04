# test_nosync

## Location
[src/test/modules/libpq_pipeline/libpq_pipeline.c:614-705](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/libpq_pipeline/libpq_pipeline.c#L614-L705)

## Overview
Tests pipeline behavior when multiple queries are dispatched without explicit sync points, validating asynchronous result processing and buffer management.

## Definition

```c
struct timeval tv;
```
## Detailed Description
The  function tests the behavior of PostgreSQL pipelines when multiple queries are sent without using explicit synchronization points (PQpipelineSync). This scenario tests the ability to queue multiple queries and process their results asynchronously while managing network buffers and socket I/O effectively.

The function performs the following test sequence:
1. Enters pipeline mode
2. Sends 10 identical parameterized SELECT queries without sync points
3. For each query sent:
   - Uses PQflush() to ensure the query is sent immediately
   - Uses select() with zero timeout to check for available input data
   - Reads any available data using PQconsumeInput() if data is ready
4. Sends a flush request to ensure the server processes all queued queries
5. Processes all results by repeatedly calling PQgetResult():
   - Expects exactly one PGRES_TUPLES_OK result per sent query
   - Expects one NULL result after each TUPLES_OK result
   - Counts results until all expected queries have been processed

This test validates that pipelines work correctly without explicit synchronization and that the client can handle asynchronous result processing with proper buffer management.

## Parameters / Member Variables
- : The database connection to test no-sync pipeline behavior on

## Dependencies
- Functions called/Symbols referenced:
  - [PQsocket](../P/PQsocket.md)
  - [PQenterPipelineMode](../P/PQenterPipelineMode.md)
  - [PQsendQueryParams](../P/PQsendQueryParams.md)
  - [PQflush](../P/PQflush.md)
  - [PQconsumeInput](../P/PQconsumeInput.md)
  - [PQsendFlushRequest](../P/PQsendFlushRequest.md)
  - [PQgetResult](../P/PQgetResult.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - [PQresStatus](../P/PQresStatus.md)
  - [PQclear](../P/PQclear.md)
  - [PQerrorMessage](../P/PQerrorMessage.md)
  - PGRES_TUPLES_OK
  - select
  - FD_ZERO
  - FD_SET
  - FD_ISSET
  - [exit_nicely](../e/exit_nicely.md)
  - fprintf
  - [pg_fatal](../p/pg_fatal.md)
- Called from (representative examples):
  - [main](../m/main.md)

## Notes and Other Information
- This is a static function within the libpq_pipeline test module
- Tests advanced asynchronous pipeline processing without sync markers
- Uses file descriptor monitoring with select() for efficient I/O handling
- Demonstrates proper buffer management with PQflush() and PQconsumeInput()
- Validates that results can be processed correctly even without explicit synchronization
- Uses PQsendFlushRequest() to ensure server-side processing of all queued queries
- Tests with 10 identical queries using SELECT repeat('xyzxz', 12) for predictable results
- Important for validating pipeline performance in high-throughput scenarios
- Located in src/test/modules/libpq_pipeline/libpq_pipeline.c at lines 614-705
- Demonstrates that explicit sync points are not always required for correct pipeline operation

## Simplified Source

```c
static void test_nosync(PGconn *conn) {
    int numqueries = 10;
    int results = 0;
    int sock = PQsocket(conn);

    fprintf(stderr, "nosync... ");

    if (sock < 0)
        pg_fatal("invalid socket");

    if (PQenterPipelineMode(conn) != 1)
        pg_fatal("could not enter pipeline mode");

    // Send multiple queries without sync points
    for (int i = 0; i < numqueries; i++) {
        if (PQsendQueryParams(conn, "SELECT repeat('xyzxz', 12)",
                              0, NULL, NULL, NULL, NULL, 0) != 1)
            pg_fatal("error sending select: %s", PQerrorMessage(conn));
        PQflush(conn);

        // Check for available input data and read if ready
        fd_set input_mask;
        struct timeval tv;
        FD_ZERO(&input_mask);
        FD_SET(sock, &input_mask);
        tv.tv_sec = 0;
        tv.tv_usec = 0;
        if (select(sock + 1, &input_mask, NULL, NULL, &tv) < 0) {
            fprintf(stderr, "select() failed: %m\n");
            exit_nicely(conn);
        }
        if (FD_ISSET(sock, &input_mask) && PQconsumeInput(conn) != 1)
            pg_fatal("failed to read from server: %s", PQerrorMessage(conn));
    }

    // Tell server to flush its output buffer
    if (PQsendFlushRequest(conn) != 1)
        pg_fatal("failed to send flush request");
    PQflush(conn);

    // Process all results
    for (;;) {
        PGresult *res = PQgetResult(conn);

        if (res == NULL)
            pg_fatal("got unexpected NULL result after %d results", results);

        if (PQresultStatus(res) == PGRES_TUPLES_OK) {
            // Expect NULL result after each TUPLES_OK
            PGresult *res2 = PQgetResult(conn);
            if (res2 != NULL)
                pg_fatal("expected NULL, got %s", PQresStatus(PQresultStatus(res2)));
            PQclear(res);
            results++;

            // Check if we're done
            if (results == numqueries)
                break;
        } else {
            pg_fatal("got unexpected %s\n", PQresStatus(PQresultStatus(res)));
        }
    }

    fprintf(stderr, "ok\n");
}
```