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
  - PQenterPipelineMode
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