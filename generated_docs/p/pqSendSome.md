# pqSendSome

## Location
src/interfaces/libpq/fe-misc.c: 810 - 978

## Overview
Sends data from the output buffer to the PostgreSQL server, implementing sophisticated flow control and deadlock prevention strategies for both blocking and non-blocking connections.

## Definition
```c
static int pqSendSome(PGconn *conn, int len)
```

## Detailed Description
The `pqSendSome` function is a critical component of libpq's output data management system that attempts to send a specified amount of data from the output buffer to the server. It implements comprehensive error handling, flow control, and deadlock prevention mechanisms essential for robust PostgreSQL client communication.

The function handles several complex scenarios: it manages write failures by setting persistent failure states while continuing to read from the server to collect error messages; implements deadlock prevention by reading incoming data when unable to send (crucial during COPY operations with server NOTICE responses); provides different behavior for blocking vs non-blocking connections; and includes platform-specific optimizations such as limiting write sizes on Windows.

The function maintains connection state integrity by tracking write failures, managing buffer contents, and ensuring proper cleanup of queued data when connection issues occur.

## Parameters / Member Variables
- `conn`: PostgreSQL connection object containing the output buffer and connection state
- `len`: Amount of data to attempt sending (typically equal to `outCount`, but may be less)

## Dependencies
- Functions called/Symbols referenced:
  - [pqReadData](pqReadData.md)
  - [pqsecure_write](pqsecure_write.md)
  - pqIsnonblocking
  - [pqWait](pqWait.md)
  - strdup, memmove (standard library)
  - [libpq_gettext](../l/libpq_gettext.md)
  - PGINVALID_SOCKET, SOCK_ERRNO, EAGAIN, EWOULDBLOCK, EINTR
- Called from (representative examples):
  - [pqPutMsgEnd](pqPutMsgEnd.md)
  - [pqFlush](pqFlush.md)

## Notes and Other Information
- Returns 0 on success, -1 on failure, 1 when not all data could be sent due to non-blocking socket constraints
- **Static function**: Only accessible within the fe-misc.c compilation unit
- **Write failure handling**: Once `conn->write_failed` is set, the function discards all outgoing data but continues reading to collect server error messages
- **Deadlock prevention**: Actively reads incoming data when send operations block to prevent deadlocks during large data transfers (especially COPY operations)
- **Platform-specific behavior**: On Windows, limits individual write operations to 64K to avoid known Windows socket issues (KB article Q201213)
- **Non-blocking support**: In non-blocking mode, returns 1 when data remains unsent rather than waiting, allowing caller to handle flow control
- **Buffer management**: Automatically shifts remaining unsent data to the beginning of the output buffer after partial sends
- **Error message priority**: Prioritizes server error messages over local write errors by continuing to read even after write failures
- The function implements a sophisticated wait strategy for blocked sends, waiting for both read and write readiness to handle bidirectional data flow
- Part of the core PostgreSQL wire protocol implementation in libpq