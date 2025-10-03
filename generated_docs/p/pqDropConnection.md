# pqDropConnection

## Location
[src/interfaces/libpq/fe-connect.c:471-557](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L471-L557)

## Overview
Closes any physical connection to the PostgreSQL server and resets associated state inside the connection object while preserving state needed for potential reconnection.

## Definition

```c
void
pqDropConnection(PGconn *conn, bool flushInput)
```
## Detailed Description
This function performs a controlled teardown of a PostgreSQL connection without fully destroying the connection object. It closes the physical socket connection, cleans up SSL/security state, and optionally discards buffered data. The function is designed to allow for potential reconnection by preserving connection parameters and other reusable state information.

The function always flushes the output buffer since there's no hope of sending that data once the connection is dropped. However, unprocessed input data might still be valuable for error reporting or debugging, so the caller can choose whether to preserve or discard it via the flushInput parameter.

## Parameters / Member Variables
- `*conn`: Pointer to the PostgreSQL connection object (PGconn) to be disconnected
- `flushInput`: Boolean flag indicating whether to discard unread input data (true) or preserve it (false)
## Dependencies
- Functions called/Symbols referenced:
  - [pqsecure_close](pqsecure_close.md)
  - closesocket
  - [pqFreeCommandQueue](pqFreeCommandQueue.md)
  - PGINVALID_SOCKET (constant)
  - Various GSS/SSPI/SASL cleanup functions (conditional compilation)

- Called from (representative examples):
  - [pqConnectDBStart](pqConnectDBStart.md)
  - [PQconnectPoll](../P/PQconnectPoll.md)
  - [pqClosePGconn](pqClosePGconn.md)
  - [pqReadData](pqReadData.md)
  - [handleSyncLoss](../h/handleSyncLoss.md)

## Notes and Other Information
- The function handles cleanup for multiple authentication mechanisms (GSS, SSPI, SASL) through conditional compilation
- Always discards unsent data in the output buffer and pending pipelined commands
- Maintains connection object structure for potential reuse, unlike full connection destruction
- Critical for implementing connection retry logic and handling network failures gracefully
- Used extensively in libpq's connection management and error recovery mechanisms

## Simplified Source

```c
void
pqDropConnection(PGconn *conn, bool flushInput)
{
    // Close SSL/secure connection
    pqsecure_close(conn);

    // Close the socket
    if (conn->sock != PGINVALID_SOCKET)
        closesocket(conn->sock);
    conn->sock = PGINVALID_SOCKET;

    // Optionally clear input buffer
    if (flushInput)
        conn->inStart = conn->inCursor = conn->inEnd = 0;

    // Always clear output buffer
    conn->outCount = 0;

    // Free pending command queue
    pqFreeCommandQueue(conn->cmd_queue_head);
    conn->cmd_queue_head = conn->cmd_queue_tail = NULL;
    pqFreeCommandQueue(conn->cmd_queue_recycle);
    conn->cmd_queue_recycle = NULL;

    // Clean up authentication state (GSS, SSPI, SASL)
#ifdef ENABLE_GSS
    // GSS cleanup code...
    if (conn->gcred != GSS_C_NO_CREDENTIAL)
        gss_release_cred(&min_s, &conn->gcred);
    // Additional GSS cleanup
#endif
#ifdef ENABLE_SSPI
    // SSPI cleanup code...
#endif
    if (conn->sasl_state)
    {
        conn->sasl->free(conn->sasl_state);
        conn->sasl_state = NULL;
    }
}
```