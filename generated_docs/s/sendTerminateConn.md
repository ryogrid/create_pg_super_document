# sendTerminateConn

## Location
src/interfaces/libpq/fe-connect.c: 4798 - 4831

## Overview
Sends a terminate message to the PostgreSQL backend to gracefully close the connection when the connection is in a valid state.

## Definition
```c
static void sendTerminateConn(PGconn *conn)
```

## Detailed Description
The `sendTerminateConn` function is responsible for sending a protocol-level terminate message to the PostgreSQL backend server to initiate a graceful connection closure. The function includes several safety checks: it skips sending terminate messages for cancellation requests (as the cancellation protocol doesn't support terminate messages), and it only sends the message when the socket is valid and the connection is in the CONNECTION_OK state. The function constructs a terminate message using the PostgreSQL wire protocol and attempts to flush it to the server, ignoring any transmission errors that might occur during this cleanup phase.

## Parameters / Member Variables
- `conn`: Pointer to the PostgreSQL connection object (PGconn) for which to send the terminate message

## Dependencies
- Functions called/Symbols referenced:
  - PGINVALID_SOCKET (constant)
  - CONNECTION_OK (constant)
  - PqMsg_Terminate (message type constant)
  - [pqPutMsgStart](../p/pqPutMsgStart.md)
  - [pqPutMsgEnd](../p/pqPutMsgEnd.md)
  - [pqFlush](../p/pqFlush.md)
- Called from (representative examples):
  - internalPQconninfoOption
  - [pqClosePGconn](../p/pqClosePGconn.md)

## Notes and Other Information
- This is a static function, only accessible within the fe-connect.c file
- The function specifically avoids sending terminate messages during cancellation requests
- Protocol restrictions prevent sending terminate messages during the startup phase
- The function ignores any errors that occur during message transmission, as this is part of cleanup logic
- Only sends the terminate message when the connection is in a healthy state (CONNECTION_OK)
- Part of the graceful connection shutdown process in libpq
- Uses the PostgreSQL wire protocol message format for communication