# libpqrcv_PQgetResult

## Location
src/backend/replication/libpqwalreceiver/libpqwalreceiver.c: 835 - 879

## Overview
Performs the equivalent of PQgetResult() but includes interrupt handling and socket readiness waiting for WAL receiver operations.

## Definition
```c
static PGresult *libpqrcv_PQgetResult(PGconn *streamConn)
```

## Detailed Description
This function wraps PQgetResult() with WAL receiver-specific interrupt handling. It waits for the connection to become ready using WaitLatchOrSocket(), which allows the function to respond to interrupts while waiting for data from the primary server. The function continuously polls the connection using PQisBusy() and consumes input with PQconsumeInput() until a complete result is available.

The function integrates with PostgreSQL's interrupt handling system by calling ProcessWalRcvInterrupts() when the latch is set, ensuring that WAL receiver operations can be interrupted gracefully for shutdown or other signals.

## Parameters / Member Variables
- `streamConn`: PostgreSQL connection object used for replication streaming

## Dependencies
- Functions called/Symbols referenced:
  - PQisBusy (to check if connection has pending operations)
  - WaitLatchOrSocket (to wait for socket readiness or latch signals)
  - PQsocket (to get the socket file descriptor)
  - ResetLatch (to reset the latch after processing)
  - ProcessWalRcvInterrupts (to handle WAL receiver interrupts)
  - PQconsumeInput (to consume available input from socket)
  - PQgetResult (to retrieve the actual result)
- Used by:
  - libpqrcv_PQexec (for collecting query results)
  - libpqrcv_endstreaming (for handling streaming termination)
  - libpqrcv_receive (for receiving WAL data)

## Notes and Other Information
- This is a static function, only accessible within libpqwalreceiver.c
- Uses PostgreSQL's latch mechanism for efficient waiting with interrupt support
- Waits for WL_EXIT_ON_PM_DEATH, WL_SOCKET_READABLE, and WL_LATCH_SET events
- Returns NULL if PQconsumeInput() fails, indicating connection trouble
- The function does not break down sleep into smaller increments since interrupts are handled properly
- Essential for maintaining responsiveness during long-running replication operations
- Integrates with WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE wait event for monitoring