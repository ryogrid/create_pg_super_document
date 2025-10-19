# libpqsrv_get_result

## Location
[src/include/libpq/libpq-be-fe-helpers.h:334-385](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/libpq/libpq-be-fe-helpers.h#L334-L385)

## Overview
Performs the equivalent of PQgetResult() while watching for interrupts and processing them appropriately during result retrieval.

## Definition
static inline PGresult *libpqsrv_get_result(PGconn *conn, uint32 wait_event_info)

## Detailed Description
This function provides an interrupt-aware wrapper around PQgetResult() by first ensuring the connection is ready to return results without blocking. It uses WaitLatchOrSocket() to efficiently wait for socket readability while monitoring for process interrupts and death signals. The function continuously consumes input from the socket until PQgetResult() can be called without blocking, making it safe for use in server contexts where responsiveness to interrupts is critical.

## Parameters / Member Variables
- conn: PostgreSQL connection handle to retrieve results from
- wait_event_info: Wait event information passed to WaitLatchOrSocket for monitoring purposes

## Dependencies
- Functions called/Symbols referenced:
  - [PQisBusy](../P/PQisBusy.md)
  - [WaitLatchOrSocket](../W/WaitLatchOrSocket.md)
  - WL_EXIT_ON_PM_DEATH
  - WL_LATCH_SET
  - WL_SOCKET_READABLE
  - [PQsocket](../P/PQsocket.md)
  - MyLatch
  - [ResetLatch](../R/ResetLatch.md)
  - CHECK_FOR_INTERRUPTS
  - [PQconsumeInput](../P/PQconsumeInput.md)
  - [PQgetResult](../P/PQgetResult.md)
- Called from (representative examples):
  - [libpqsrv_get_result_last](libpqsrv_get_result_last.md)

## Notes and Other Information
- Uses PostgreSQL's latch mechanism for efficient interrupt-aware waiting
- Handles three types of events: process death, latch signals, and socket readability
- Calls CHECK_FOR_INTERRUPTS() when interrupted to process pending signals
- Continues consuming socket input until the connection is no longer busy
- Returns NULL if PQconsumeInput() fails, indicating connection problems
- Located in src/include/libpq/libpq-be-fe-helpers.h:334-385

## Simplified Source

```c
static inline PGresult *libpqsrv_get_result(PGconn *conn, uint32 wait_event_info) {
    // Wait until connection is ready for non-blocking result retrieval
    while (PQisBusy(conn)) {
        // Wait for socket readability or interrupts
        int rc = WaitLatchOrSocket(MyLatch,
                                 WL_EXIT_ON_PM_DEATH | WL_LATCH_SET | WL_SOCKET_READABLE,
                                 PQsocket(conn), 0, wait_event_info);

        // Handle interrupts
        if (rc & WL_LATCH_SET) {
            ResetLatch(MyLatch);
            CHECK_FOR_INTERRUPTS();
        }

        // Consume available data from socket
        if (PQconsumeInput(conn) == 0) {
            // Connection trouble - PQgetResult() will return NULL
            break;
        }
    }

    // Now get the result without blocking
    return PQgetResult(conn);
}
```