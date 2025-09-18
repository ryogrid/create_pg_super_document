# libpqsrv_cancel

## Location
src/include/libpq/libpq-be-fe-helpers.h: 386 - 457

## Overview
Submits a cancel request to a PostgreSQL connection with timeout support and interrupt handling, providing safe query cancellation in server contexts.

## Definition
static inline const char *libpqsrv_cancel(PGconn *conn, TimestampTz endtime)

## Detailed Description
This function provides a robust mechanism for canceling ongoing PostgreSQL operations with proper timeout handling and interrupt processing. It creates a cancel connection using PQcancelCreate(), initiates the cancel request with PQcancelStart(), and then polls for completion using PQcancelPoll() while respecting the specified timeout. The function handles both socket reading and writing states during the cancellation process, ensuring proper cleanup through PostgreSQL's PG_TRY/PG_FINALLY mechanism to prevent resource leaks.

## Parameters / Member Variables
- conn: PostgreSQL connection handle for which to send the cancel request
- endtime: Timestamp indicating when the cancel operation should timeout

## Dependencies
- Functions called/Symbols referenced:
  - PGcancelConn
  - PQcancelCreate
  - PG_TRY
  - PQcancelStart
  - pchomp
  - PQcancelErrorMessage
  - PostgresPollingStatusType
  - PQcancelPoll
  - PGRES_POLLING_OK
  - GetCurrentTimestamp
  - TimestampDifferenceMilliseconds
  - PGRES_POLLING_READING
  - PGRES_POLLING_WRITING
  - WL_LATCH_SET
  - WL_TIMEOUT
  - WL_EXIT_ON_PM_DEATH
  - WL_SOCKET_READABLE
  - WL_SOCKET_WRITEABLE
  - WaitLatchOrSocket
  - PQcancelSocket
  - PG_WAIT_CLIENT
  - MyLatch
  - ResetLatch
  - CHECK_FOR_INTERRUPTS
  - PG_FINALLY
  - PQcancelFinish
  - PG_END_TRY
- Called from (representative examples):
  - No direct references found in current codebase

## Notes and Other Information
- Returns NULL on successful cancellation, or an error message string on failure
- Can ereport(ERROR) for severe problems like out-of-memory conditions
- Should be called within a transient memory context due to potential string leaks
- Handles timeout by calculating remaining time at each polling iteration
- Uses PostgreSQL's polling mechanism to handle both reading and writing socket states
- Guaranteed resource cleanup through PG_FINALLY block even in error conditions
- Located in src/include/libpq/libpq-be-fe-helpers.h:386-457