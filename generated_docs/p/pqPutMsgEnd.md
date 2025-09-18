# pqPutMsgEnd

## Location
src/interfaces/libpq/fe-misc.c: 517 - 590

## Overview
Finishes constructing a PostgreSQL protocol message by filling in the length field and optionally sending the accumulated data when the buffer reaches a threshold size.

## Definition
```c
int pqPutMsgEnd(PGconn *conn)
```

## Detailed Description
The `pqPutMsgEnd` function completes the construction of a PostgreSQL protocol message by calculating and filling in the message length field at the beginning of the message. It then makes the message eligible for sending by updating the connection's output count. 

The function implements an intelligent buffering strategy: it automatically attempts to send data when the output buffer accumulates at least 8K worth of data (typical pipe buffer size on Unix systems). This optimization reduces the number of small partial packets sent over the network. For complete control over when data is sent, callers must use `pqFlush`.

The function includes special handling for Unix domain socket connections where it prefers sending pipe-buffer-sized packets for better performance, with important safety considerations for SSL/GSSAPI connections.

## Parameters / Member Variables
- `conn`: PostgreSQL connection object containing the output buffer, message boundaries, and connection state

## Dependencies
- Functions called/Symbols referenced:
  - pg_hton32 (host-to-network byte order conversion)
  - pqTraceOutputMessage
  - pqTraceOutputNoTypeByteMessage
  - pqSendSome
  - memcpy (standard library)
- Called from (representative examples):
  - pg_SASL_init
  - sendTerminateConn
  - pqPacketSend
  - PQsendQueryInternal
  - PQsendPrepare
  - PQsendQueryGuts
  - PQsendTypedCommand
  - PQputCopyData
  - PQputCopyEnd

## Notes and Other Information
- Returns 0 on success, EOF on error
- Automatically fills in the 4-byte message length field in network byte order when `conn->outMsgStart >= 0`
- Implements debug tracing for client-to-server messages when `conn->Pfdebug` is enabled
- Uses 8192 bytes (8K) as the threshold for automatic sending to optimize network efficiency
- Contains special logic for Unix domain sockets to send data in pipe-buffer-sized chunks
- Includes safety assertions for SSL and GSSAPI connections on Unix sockets
- In non-blocking mode, does not complain if unable to send all data immediately
- This function is part of the core PostgreSQL wire protocol implementation in libpq