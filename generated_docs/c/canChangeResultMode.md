# canChangeResultMode

## Location
[src/interfaces/libpq/fe-exec.c:1925-1947](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L1925-L1947)

## Overview
canChangeResultMode is a static utility function that determines whether it's safe to change the partial-result mode for a PostgreSQL connection at the current time.

## Definition

```c
static bool
canChangeResultMode(PGconn *conn)
```
## Detailed Description
This function performs validation checks to determine if the connection state allows changing the result mode (such as switching to single-row mode or chunked rows mode). The function ensures that mode changes only occur at the appropriate time in the query execution lifecycle - specifically after a query has been launched but before any results have been received.

The function enforces several safety conditions:
1. The connection must be valid (not NULL)
2. The connection must be in BUSY state (actively processing a query)
3. There must be a queued command that is either a simple or extended query
4. No results should be pending for processing

This validation prevents mode changes that could corrupt the result stream or cause protocol violations.

## Parameters / Member Variables
- : The PostgreSQL connection handle to check

## Dependencies
- Functions called/Symbols referenced:
  - pgHavePendingResult
  - PGASYNC_BUSY (connection status constant)
  - PGQUERY_SIMPLE, PGQUERY_EXTENDED (query class constants)
- Called from (representative examples):
  - [PQsetSingleRowMode](../P/PQsetSingleRowMode.md)
  - [PQsetChunkedRowsMode](../P/PQsetChunkedRowsMode.md)

## Notes and Other Information
- This is a static function used internally by libpq for validation purposes
- The function is primarily used by result mode setting functions to ensure safe state transitions
- Returns true only when all safety conditions are met, false otherwise
- The function checks the command queue to ensure there's an active query of the appropriate type
- Part of the result mode management system that allows clients to control how results are delivered