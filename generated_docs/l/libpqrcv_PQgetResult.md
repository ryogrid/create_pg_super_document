# libpqrcv_PQgetResult

## Location
[src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:835-879](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/libpqwalreceiver/libpqwalreceiver.c#L835-L879)

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
  - [PQisBusy](../P/PQisBusy.md) (to check if connection has pending operations)
  - [WaitLatchOrSocket](../W/WaitLatchOrSocket.md) (to wait for socket readiness or latch signals)
  - [PQsocket](../P/PQsocket.md) (to get the socket file descriptor)
  - [ResetLatch](../R/ResetLatch.md) (to reset the latch after processing)
  - [ProcessWalRcvInterrupts](../P/ProcessWalRcvInterrupts.md) (to handle WAL receiver interrupts)
  - [PQconsumeInput](../P/PQconsumeInput.md) (to consume available input from socket)
  - [PQgetResult](../P/PQgetResult.md) (to retrieve the actual result)
- Used by:
  - [libpqrcv_PQexec](libpqrcv_PQexec.md) (for collecting query results)
  - [libpqrcv_endstreaming](libpqrcv_endstreaming.md) (for handling streaming termination)
  - [libpqrcv_receive](libpqrcv_receive.md) (for receiving WAL data)

## Notes and Other Information
- This is a static function, only accessible within libpqwalreceiver.c
- Uses PostgreSQL's latch mechanism for efficient waiting with interrupt support
- Waits for WL_EXIT_ON_PM_DEATH, WL_SOCKET_READABLE, and WL_LATCH_SET events
- Returns NULL if PQconsumeInput() fails, indicating connection trouble
- The function does not break down sleep into smaller increments since interrupts are handled properly
- Essential for maintaining responsiveness during long-running replication operations
- Integrates with WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE wait event for monitoring

## Simplified Source

```c
static PGresult *
libpqrcv_PQgetResult(PGconn *streamConn)
{
    // Wait until connection is ready for result retrieval
    while (PQisBusy(streamConn)) {
        int rc;

        // Wait for socket readiness or latch signal
        rc = WaitLatchOrSocket(MyLatch,
                               WL_EXIT_ON_PM_DEATH | WL_SOCKET_READABLE | WL_LATCH_SET,
                               PQsocket(streamConn),
                               0,
                               WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE);

        // Handle interrupts
        if (rc & WL_LATCH_SET) {
            ResetLatch(MyLatch);
            ProcessWalRcvInterrupts();
        }

        // Consume available socket data
        if (PQconsumeInput(streamConn) == 0) {
            return NULL;  // Connection trouble
        }
    }

    // Retrieve the result
    return PQgetResult(streamConn);
}
```