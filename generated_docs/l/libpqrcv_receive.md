# libpqrcv_receive

## Location
[src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:904-993](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/libpqwalreceiver/libpqwalreceiver.c#L904-L993)

## Overview
Receives WAL messages from the XLOG stream during replication, handling both data reception and end-of-stream conditions.

## Definition
```c
static int libpqrcv_receive(WalReceiverConn *conn, char **buffer, pgsocket *wait_fd)
```

## Detailed Description
This function is the core data reception mechanism for WAL streaming replication. It attempts to receive CopyData messages from the primary server using PQgetCopyData(). The function handles three main scenarios: successful data reception, no data immediately available (requiring waiting), and end-of-streaming conditions.

The function implements a two-step approach: first attempting to receive data directly, then consuming input and trying again if no data was immediately available. It provides comprehensive error handling for protocol violations and connection failures, and properly manages the receive buffer lifecycle.

## Parameters / Member Variables
- `conn`: WAL receiver connection object containing the stream connection and receive buffer
- `buffer`: Output parameter that receives a pointer to the received message data
- `wait_fd`: Output parameter that receives the socket descriptor for waiting when no data is available

## Dependencies
- Functions called/Symbols referenced:
  - [PQfreemem](../P/PQfreemem.md) (to free previous receive buffer)
  - [PQgetCopyData](../P/PQgetCopyData.md) (to receive COPY data from the stream)
  - [PQconsumeInput](../P/PQconsumeInput.md) (to consume available input data)
  - [PQsocket](../P/PQsocket.md) (to get socket descriptor for waiting)
  - [libpqrcv_PQgetResult](libpqrcv_PQgetResult.md) (to get command results on stream end)
  - [PQresultStatus](../P/PQresultStatus.md) (to check result status)
  - [PQstatus](../P/PQstatus.md) (to check connection status)
  - [PQclear](../P/PQclear.md) (to clean up result objects)
  - [pchomp](../p/pchomp.md) (to format error messages)
- Used by:
  - WAL receiver main loop for continuous data reception
  - Replication stream processing functions

## Notes and Other Information
- This is a static function, only accessible within libpqwalreceiver.c
- Returns positive integer (data length) on successful data reception
- Returns 0 when no data is immediately available, with wait_fd set for polling
- Returns -1 on end-of-streaming or orderly connection closure
- Throws ereport(ERROR) on protocol violations or connection failures
- The returned buffer is only valid until the next libpqrcv_* function call
- Handles both normal end-of-stream (PGRES_COMMAND_OK) and error conditions
- Automatically manages receive buffer memory by freeing previous buffer before each call
- Critical component of PostgreSQL's streaming replication infrastructure

## Simplified Source

```c
static int
libpqrcv_receive(WalReceiverConn *conn, char **buffer, pgsocket *wait_fd)
{
    int rawlen;

    // Free previous receive buffer
    PQfreemem(conn->recvBuf);
    conn->recvBuf = NULL;

    // Try to receive COPY data
    rawlen = PQgetCopyData(conn->streamConn, &conn->recvBuf, 1);
    if (rawlen == 0) {
        // No data available, try consuming input first
        if (PQconsumeInput(conn->streamConn) == 0)
            ereport(ERROR, (errmsg("could not receive data from WAL stream: %s",
                                   pchomp(PQerrorMessage(conn->streamConn)))));

        // Try again after consuming input
        rawlen = PQgetCopyData(conn->streamConn, &conn->recvBuf, 1);
        if (rawlen == 0) {
            // Still no data, return socket for waiting
            *wait_fd = PQsocket(conn->streamConn);
            return 0;
        }
    }

    if (rawlen == -1) {
        // End of streaming or error
        PGresult *res = libpqrcv_PQgetResult(conn->streamConn);

        if (PQresultStatus(res) == PGRES_COMMAND_OK) {
            PQclear(res);

            // Verify no additional results
            res = libpqrcv_PQgetResult(conn->streamConn);
            if (res != NULL) {
                PQclear(res);
                if (PQstatus(conn->streamConn) == CONNECTION_BAD)
                    return -1;
                ereport(ERROR, (errmsg("unexpected result after CommandComplete: %s",
                                       PQerrorMessage(conn->streamConn))));
            }
            return -1;
        } else if (PQresultStatus(res) == PGRES_COPY_IN) {
            PQclear(res);
            return -1;
        } else {
            PQclear(res);
            ereport(ERROR, (errmsg("could not receive data from WAL stream: %s",
                                   pchomp(PQerrorMessage(conn->streamConn)))));
        }
    }

    if (rawlen < -1)
        ereport(ERROR, (errmsg("could not receive data from WAL stream: %s",
                               pchomp(PQerrorMessage(conn->streamConn)))));

    // Return received data
    *buffer = conn->recvBuf;
    return rawlen;
}
```