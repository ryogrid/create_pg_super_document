# PQflush

## Location
src/interfaces/libpq/fe-exec.c: 4000 - 4015

## Overview
Forces any pending data in the connection's output buffer to be sent to the PostgreSQL server, primarily useful for applications using non-blocking I/O operations.

## Definition
```c
int PQflush(PGconn *conn)
```

## Detailed Description
PQflush attempts to flush any data waiting in the connection's output buffer to the server. This function is particularly important when using non-blocking I/O operations, as it provides explicit control over when buffered data is transmitted to the server.

The function serves as a public API wrapper around the internal pqFlush function, adding connection validity checks. In non-blocking mode, applications typically need to call PQflush to ensure commands and data are actually sent to the server, as they may remain in local buffers otherwise.

When used with blocking connections, PQflush may block until the data is successfully sent or an error occurs. With non-blocking connections, it will return immediately indicating whether the flush succeeded, failed, or needs to be retried.

## Parameters / Member Variables
- `conn`: PostgreSQL connection object (PGconn pointer) - the connection whose output buffer should be flushed

## Dependencies
- Functions called/Symbols referenced:
  - CONNECTION_BAD (connection status constant)
  - [pqFlush](../p/pqFlush.md) (internal flush implementation)
- Called from (representative examples):
  - [libpqrcv_endstreaming](../l/libpqrcv_endstreaming.md) (in libpqwalreceiver)
  - [libpqrcv_send](../l/libpqrcv_send.md) (in libpqwalreceiver)
  - [sendFeedback](../s/sendFeedback.md) (in pg_recvlogical and receivelog)
  - [test_nosync](../t/test_nosync.md), test_pipelined_insert, test_uniqviol (in libpq_pipeline test module)

## Notes and Other Information
- Returns 0 if the flush succeeded or if there was no data to flush
- Returns 1 if the flush failed but should be retried (non-blocking mode only)
- Returns -1 on error or if the connection is invalid/bad
- Most useful in non-blocking applications where explicit buffer control is needed
- May be called automatically by other libpq functions when necessary
- Essential for pipeline operations and streaming replication scenarios
- In blocking mode, the function will wait until data is sent or an error occurs
- Should be called after sending commands in non-blocking mode to ensure delivery