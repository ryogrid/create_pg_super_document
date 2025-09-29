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

## Simplified Source
```c
int pqReadData(PGconn *conn) {
    int someread = 0;
    int nread;

    // Validate socket
    if (conn->sock == PGINVALID_SOCKET) {
        libpq_append_conn_error(conn, "connection not open");
        return -1;
    }

    // Compact buffer by moving data to beginning
    if (conn->inStart < conn->inEnd) {
        if (conn->inStart > 0) {
            memmove(conn->inBuffer, conn->inBuffer + conn->inStart,
                   conn->inEnd - conn->inStart);
            conn->inEnd -= conn->inStart;
            conn->inCursor -= conn->inStart;
            conn->inStart = 0;
        }
    } else {
        // Reset empty buffer
        conn->inStart = conn->inCursor = conn->inEnd = 0;
    }

    // Enlarge buffer if nearly full (8K threshold)
    if (conn->inBufSize - conn->inEnd < 8192) {
        if (pqCheckInBufferSpace(conn->inEnd + 8192, conn)) {
            if (conn->inBufSize - conn->inEnd < 100)
                return -1;  // Need minimum space
        }
    }

    // Main read loop with retry for long messages
retry_read:
    nread = pqsecure_read(conn, conn->inBuffer + conn->inEnd,
                         conn->inBufSize - conn->inEnd);

    if (nread < 0) {
        // Handle read errors
        if (SOCK_ERRNO == EINTR)
            goto retry_read;
        if (SOCK_ERRNO == EAGAIN || SOCK_ERRNO == EWOULDBLOCK)
            return someread;
        if (/* connection failure */)
            goto connection_failed;
        return -1;  // Other error
    }

    if (nread > 0) {
        conn->inEnd += nread;

        // Optimization: retry for long messages to avoid O(N²) performance
        if (conn->inEnd > 32768 && (conn->inBufSize - conn->inEnd) >= 8192) {
            someread = 1;
            goto retry_read;
        }
        return 1;
    }

    if (someread)
        return 1;  // Got data in previous iterations

    // Zero read: check if it's EOF or just no data available
#ifdef USE_SSL
    if (conn->ssl_in_use)
        return 0;  // SSL handles EOF detection differently
#endif

    // Check if socket has data ready
    switch (pqReadReady(conn)) {
        case 0: return 0;      // No data available
        case 1: break;         // Data ready, try again
        default: goto connection_eof;  // Error or EOF
    }

    // One more read attempt to distinguish EOF from temporary unavailability
    nread = pqsecure_read(conn, conn->inBuffer + conn->inEnd,
                         conn->inBufSize - conn->inEnd);
    if (nread > 0) {
        conn->inEnd += nread;
        return 1;
    }

connection_eof:
    libpq_append_conn_error(conn, "server closed the connection unexpectedly");

connection_failed:
    pqDropConnection(conn, false);  // Keep existing data
    conn->status = CONNECTION_BAD;
    return -1;
}
```