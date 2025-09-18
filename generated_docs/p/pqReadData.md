# pqReadData

## Location
[src/interfaces/libpq/fe-misc.c:591-809](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-misc.c#L591-L809)

## Overview
Reads incoming data from the PostgreSQL server connection into the input buffer, implementing intelligent buffering and error handling strategies for optimal network performance.

## Definition
```c
int pqReadData(PGconn *conn)
```

## Detailed Description
The `pqReadData` function is a core component of libpq's network I/O system that attempts to read available data from the server connection. It implements sophisticated buffer management, including automatic buffer enlargement, left-justification of existing data, and intelligent retry logic for handling partial reads.

The function includes several important optimizations: it automatically enlarges the input buffer when nearly full (with 8K threshold), implements a retry mechanism for long messages to achieve O(N) instead of O(N²) performance, and handles both blocking and non-blocking I/O modes. It also includes comprehensive error handling for various network conditions including connection failures, EOF detection, and platform-specific socket errors.

The function carefully distinguishes between temporary unavailability of data (returning 0) and actual connection closure or errors (returning -1), making it suitable for both synchronous and asynchronous operation modes.

## Parameters / Member Variables
- `conn`: PostgreSQL connection object containing the socket, input buffer, and connection state information

## Dependencies
- Functions called/Symbols referenced:
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md)
  - memmove (standard library)
  - [pqCheckInBufferSpace](pqCheckInBufferSpace.md)
  - [pqsecure_read](pqsecure_read.md)
  - [pqReadReady](pqReadReady.md)
  - [pqDropConnection](pqDropConnection.md)
  - PGINVALID_SOCKET, SOCK_ERRNO, EINTR, EAGAIN, EWOULDBLOCK
  - ALL_CONNECTION_FAILURE_ERRNOS, USE_SSL, CONNECTION_BAD
- Called from (representative examples):
  - [PQcancelPoll](../P/PQcancelPoll.md)
  - [PQconnectPoll](../P/PQconnectPoll.md)
  - CONNECTION_FAILED
  - [PQconsumeInput](../P/PQconsumeInput.md)
  - [PQgetResult](../P/PQgetResult.md)
  - [pqSendSome](pqSendSome.md)
  - [pqGetCopyData3](pqGetCopyData3.md)
  - [pqGetline3](pqGetline3.md)
  - [pqFunctionCall3](pqFunctionCall3.md)

## Notes and Other Information
- Returns 1 if at least one byte was successfully read, 0 if no data is available but no error occurred, -1 on error or EOF
- **CRITICAL**: Callers must not assume that pointers or indexes into `conn->inBuffer` remain valid across this call due to potential buffer reallocation
- Implements left-justification of buffer data to maximize available space for new reads
- Uses 8192 bytes as the threshold for buffer enlargement and considers messages 'long' after 32K
- Includes special handling for SSL connections where EOF detection is more complex
- Implements retry logic with `goto retry3` and `retry4` labels for handling interrupted system calls and optimizing long message reads
- Platform-specific error handling for EAGAIN/EWOULDBLOCK and connection failure scenarios
- Sets appropriate error messages and connection status on failures
- Does not drop already-read data when connection fails, allowing caller to process any remaining data
- Part of the core PostgreSQL wire protocol implementation in libpq