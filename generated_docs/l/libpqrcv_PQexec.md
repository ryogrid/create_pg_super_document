# libpqrcv_PQexec

## Location
src/backend/replication/libpqwalreceiver/libpqwalreceiver.c: 786 - 834

## Overview
Sends a query to the primary server and waits for results using asynchronous libpq functions, specifically designed for WAL receiver operations.

## Definition
```c
static PGresult *libpqrcv_PQexec(PGconn *streamConn, const char *query)
```

## Detailed Description
This function is modeled after libpqsrv_exec() but adapted for WAL receiver operations. It sends a query using PQsendQuery() and then iteratively collects results using libpqrcv_PQgetResult(). The function handles multiple result sets by returning the last result, similar to PQexec()'s behavior. It includes special handling for COPY operations and connection status checks.

As an optimization, the function skips try/catch error handling since all errors in the WAL receiver context terminate the process. The function may return NULL on failure rather than an error result.

## Parameters / Member Variables
- `streamConn`: PostgreSQL connection object used for replication streaming
- `query`: SQL command string to execute on the primary server

## Dependencies
- Functions called/Symbols referenced:
  - [PQsendQuery](../P/PQsendQuery.md) (for submitting the query asynchronously)
  - [libpqrcv_PQgetResult](libpqrcv_PQgetResult.md) (for collecting query results)
  - [PQclear](../P/PQclear.md) (for cleaning up previous results)
  - [PQresultStatus](../P/PQresultStatus.md) (for checking result status)
  - PQstatus (for checking connection status)
- Used by:
  - [libpqrcv_connect](libpqrcv_connect.md) (for connection setup queries)
  - [libpqrcv_identify_system](libpqrcv_identify_system.md) (for system identification)
  - [libpqrcv_startstreaming](libpqrcv_startstreaming.md) (for starting replication)
  - [libpqrcv_readtimelinehistoryfile](libpqrcv_readtimelinehistoryfile.md) (for timeline history retrieval)
  - [libpqrcv_create_slot](libpqrcv_create_slot.md) (for replication slot creation)
  - [libpqrcv_alter_slot](libpqrcv_alter_slot.md) (for replication slot modification)
  - [libpqrcv_exec](libpqrcv_exec.md) (for general query execution)

## Notes and Other Information
- This is a static function, only accessible within libpqwalreceiver.c
- The function does not use non-blocking mode, but blocking risk is considered negligible for short query strings
- Handles COPY operations (PGRES_COPY_IN, PGRES_COPY_OUT, PGRES_COPY_BOTH) as terminal conditions
- Returns the last result when multiple results are available, discarding intermediate ones
- Designed specifically for WAL receiver operations with ProcessWalRcvInterrupts() integration mindset
- May return NULL instead of error results, unlike standard PQexec()