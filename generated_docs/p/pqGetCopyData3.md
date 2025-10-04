# pqGetCopyData3

## Location
[src/interfaces/libpq/fe-protocol3.c:1751-1809](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-protocol3.c#L1751-L1809)

## Overview
Reads a row of data from the PostgreSQL backend during COPY OUT or COPY BOTH operations, implementing the protocol 3.0 version of copy data retrieval.

## Definition
```c
int pqGetCopyData3(PGconn *conn, char **buffer, int async)
```

## Detailed Description
The pqGetCopyData3 function is the core implementation for retrieving data during PostgreSQL COPY operations. It handles the protocol-level details of reading CopyData messages from the network stream and presenting them to the application as malloc'd data buffers. The function supports both synchronous and asynchronous operation modes.

The function operates in a loop, using getCopyDataMessage to handle protocol-level message processing, then allocating memory for the data payload and copying it from the network buffer to a newly allocated buffer that becomes owned by the caller. It properly handles network I/O blocking in synchronous mode and implements proper error handling including memory allocation failures.

## Parameters / Member Variables
- `conn`: PostgreSQL connection object containing the network stream and connection state
- `buffer`: Output parameter that will point to malloc'd row data on success
- `async`: If true, function returns immediately when no data is available rather than blocking

## Dependencies
- Functions called/Symbols referenced:
  - [getCopyDataMessage](../g/getCopyDataMessage.md)
  - [pqWait](pqWait.md)
  - [pqReadData](pqReadData.md)
  - malloc
  - memcpy
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md)
- Called from (representative examples):
  - [PQgetCopyData](../P/PQgetCopyData.md) (public API wrapper)

## Notes and Other Information
- Returns: row length (> 0) on success, 0 if no data available (async mode only), -1 if end of copy, -2 if error
- Caller is responsible for freeing the malloc'd buffer returned via the buffer parameter
- Automatically adds null terminator to returned data for convenience
- Handles zero-length messages by dropping them and continuing
- Supports both blocking and non-blocking operation modes
- Implements proper memory management and error reporting
- Critical component of PostgreSQL's COPY protocol implementation in libpq

## Simplified Source

```c
int pqGetCopyData3(PGconn *conn, char **buffer, int async) {
    int msgLength;

    for (;;) {
        // Get the next copy data message from network stream
        msgLength = getCopyDataMessage(conn);

        if (msgLength < 0)
            return msgLength;  // End of copy or error

        if (msgLength == 0) {
            // No data available yet
            if (async)
                return 0;  // Don't block in async mode

            // Wait for more data and retry
            if (pqWait(true, false, conn) || pqReadData(conn) < 0)
                return -2;  // Network error
            continue;
        }

        // Process the message data (subtract 4-byte message header)
        msgLength -= 4;
        if (msgLength > 0) {
            // Allocate buffer for the row data
            *buffer = malloc(msgLength + 1);
            if (*buffer == NULL) {
                libpq_append_conn_error(conn, "out of memory");
                return -2;
            }

            // Copy data from connection buffer to allocated buffer
            memcpy(*buffer, &conn->inBuffer[conn->inCursor], msgLength);
            (*buffer)[msgLength] = '\0';  // Null-terminate

            // Mark message as consumed
            conn->inStart = conn->inCursor + msgLength;
            return msgLength;
        }

        // Empty message - drop it and try again
        conn->inStart = conn->inCursor;
    }
}
```