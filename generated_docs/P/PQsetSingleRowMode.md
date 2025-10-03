# PQsetSingleRowMode

## Location
[src/interfaces/libpq/fe-exec.c:1948-1964](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L1948-L1964)

## Overview
PQsetSingleRowMode enables single-row processing mode for a PostgreSQL connection, allowing results to be retrieved one row at a time instead of waiting for the complete result set.

## Definition

```c
int
PQsetSingleRowMode(PGconn *conn)
```
## Detailed Description
PQsetSingleRowMode is a public libpq function that switches the connection to single-row mode, where query results are delivered one row at a time rather than as complete result sets. This mode is particularly useful for processing large result sets that might not fit in memory, as it allows the client application to process rows incrementally without buffering the entire result.

When single-row mode is enabled, the client must call PQgetResult() repeatedly to retrieve each row individually. Each row is returned as a separate PGresult object containing exactly one row. The final call to PQgetResult() will return NULL to indicate the end of results.

The function validates that it's safe to change the result mode using canChangeResultMode() before making the switch. It can only be called after a query has been sent but before any results have been retrieved.

## Parameters / Member Variables
- `*conn`: The PostgreSQL connection handle
## Dependencies
- Functions called/Symbols referenced:
  - [canChangeResultMode](../c/canChangeResultMode.md)
- Called from (representative examples):
  - [process_queued_fetch_requests](../p/process_queued_fetch_requests.md) (in pg_rewind)
  - [test_pipeline_abort](../t/test_pipeline_abort.md) (in libpq_pipeline tests)
  - [test_singlerowmode](../t/test_singlerowmode.md) (in libpq_pipeline tests)

## Notes and Other Information
- Returns 1 for success, 0 for failure
- Sets conn->partialResMode = true to enable partial result processing
- Sets conn->singleRowMode = true to specifically enable single-row behavior
- Sets conn->maxChunkSize = 1 to limit each result to exactly one row
- Must be called after sending a query but before retrieving any results
- Particularly useful for large result sets to reduce memory usage
- Part of libpq's partial result mode system introduced to handle large datasets efficiently
- Cannot be used with already-retrieved results or when no query is active