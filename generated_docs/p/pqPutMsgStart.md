# pqPutMsgStart

## Location
src/interfaces/libpq/fe-misc.c: 458 - 493

## Overview
Begins construction of a new protocol message in the output buffer by setting up message headers and tracking pointers.

## Definition
```c
int pqPutMsgStart(char msg_type, PGconn *conn)
```

## Detailed Description
The `pqPutMsgStart` function initiates the construction of a PostgreSQL protocol message in the connection's output buffer. It handles the setup for both regular messages (which have a type byte) and startup messages (which do not). The function reserves space for the message type byte (if applicable) and the 4-byte message length field, then sets up tracking pointers that will be used during message construction.

This function works in conjunction with `pqPutMsgEnd` to implement a message construction framework. The length field is initially left uninitialized and will be filled in by `pqPutMsgEnd` when the message is complete. The function establishes the `outMsgStart` and `outMsgEnd` state variables that track the message boundaries during construction.

## Parameters / Member Variables
- `msg_type`: The message type byte (single character identifying the message type), or 0 for messages without a type byte (startup messages only)
- `conn`: PostgreSQL connection object containing the output buffer and state variables

## Dependencies
- Functions called/Symbols referenced:
  - [pqCheckOutBufferSpace](pqCheckOutBufferSpace.md) (ensures sufficient buffer space for the message header)
- Called from (representative examples):
  - [PQsendQueryInternal](../P/PQsendQueryInternal.md) (sending SQL queries)
  - [PQsendPrepare](../P/PQsendPrepare.md) (sending prepared statement definitions)
  - [PQsendQueryGuts](../P/PQsendQueryGuts.md) (sending parameterized queries)
  - [pqFunctionCall3](pqFunctionCall3.md) (sending function call requests)
  - [PQputCopyData](../P/PQputCopyData.md) (sending COPY data)
  - [sendTerminateConn](../s/sendTerminateConn.md) (sending connection termination)

## Notes and Other Information
- Returns 0 on success, EOF on error (buffer allocation failure)
- Reserves exactly 5 bytes for regular messages (1 byte type + 4 bytes length) or 4 bytes for startup messages (4 bytes length only)
- Sets conn->outMsgStart to point to the beginning of the length field
- Sets conn->outMsgEnd to point past the reserved header space, ready for message body data
- The message length field is not filled until pqPutMsgEnd is called
- Critical component of the libpq message construction protocol
- Must be paired with pqPutMsgEnd to complete message construction
- Used extensively throughout libpq for constructing all types of client-to-server messages