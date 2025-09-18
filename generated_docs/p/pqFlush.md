# pqFlush

## Location
src/interfaces/libpq/fe-misc.c: 979 - 1003

## Overview
Forces transmission of all data waiting in the output buffer to the PostgreSQL server, providing explicit control over when buffered data is sent.

## Definition
```c
int pqFlush(PGconn *conn)
```

## Detailed Description
The `pqFlush` function provides a simple but essential interface for forcing the transmission of all accumulated data in the connection's output buffer. Unlike `pqPutMsgEnd` which only sends data when the buffer reaches a certain threshold (8K), `pqFlush` attempts to send all queued data immediately regardless of the buffer size.

This function is crucial for ensuring that important messages are sent promptly rather than being held in the buffer waiting for more data to accumulate. It's particularly important in interactive scenarios, pipeline mode operations, and when switching connection modes where buffered data must be transmitted before the operation can proceed.

The function includes debug support by flushing the debug output stream when tracing is enabled, ensuring that debug output is synchronized with actual network transmission.

## Parameters / Member Variables
- `conn`: PostgreSQL connection object containing the output buffer and connection state

## Dependencies
- Functions called/Symbols referenced:
  - [pqSendSome](pqSendSome.md)
  - fflush (standard library)
- Called from (representative examples):
  - [pg_SASL_init](pg_SASL_init.md)
  - [sendTerminateConn](../s/sendTerminateConn.md)
  - [pqPacketSend](pqPacketSend.md)
  - [PQsendQueryInternal](../P/PQsendQueryInternal.md)
  - [PQconsumeInput](../P/PQconsumeInput.md)
  - [PQgetResult](../P/PQgetResult.md)
  - [PQputCopyData](../P/PQputCopyData.md)
  - [PQputCopyEnd](../P/PQputCopyEnd.md)
  - PQexitPipelineMode
  - [pqPipelineSyncInternal](pqPipelineSyncInternal.md)
  - [PQsetnonblocking](../P/PQsetnonblocking.md)
  - [PQflush](../P/PQflush.md)
  - [pqPipelineFlush](pqPipelineFlush.md)
  - [pqEndcopy3](pqEndcopy3.md)
  - [pqFunctionCall3](pqFunctionCall3.md)

## Notes and Other Information
- Returns 0 on success, -1 on failure, 1 when not all data could be sent due to non-blocking socket constraints
- **No-op optimization**: If `conn->outCount` is 0 (no data to send), returns 0 immediately without any network operations
- **Debug synchronization**: Flushes the debug output stream (`conn->Pfdebug`) before sending data to ensure debug output appears in correct order
- **Complete transmission**: Unlike `pqPutMsgEnd` which may leave data buffered, `pqFlush` attempts to send the entire output buffer contents
- **Error handling**: Inherits the sophisticated error handling behavior from `pqSendSome`, including write failure management and deadlock prevention
- **Non-blocking support**: Works correctly with both blocking and non-blocking connections, returning appropriate status codes
- **Pipeline operations**: Essential for pipeline mode where explicit flushing ensures commands are sent at appropriate boundaries
- **Connection mode changes**: Used when switching connection properties (like blocking mode) to ensure all pending data is transmitted first
- Part of the public libpq API through `PQflush` wrapper function
- Widely used throughout libpq for ensuring timely transmission of critical protocol messages