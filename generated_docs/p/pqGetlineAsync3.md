# pqGetlineAsync3

## Location
[src/interfaces/libpq/fe-protocol3.c:1861-1915](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-protocol3.c#L1861-L1915)

## Overview
Asynchronously retrieves COPY data rows without blocking, implementing the protocol 3 version of PostgreSQL's COPY OUT mechanism for libpq client library.

## Definition

```c
int
pqGetlineAsync3(PGconn *conn, char *buffer, int bufsize)
```
## Detailed Description
pqGetlineAsync3 is the protocol 3 implementation of PostgreSQL's asynchronous COPY data retrieval mechanism. It reads COPY data from the connection's input buffer without blocking, making it suitable for non-blocking I/O operations. The function handles partial message consumption by tracking already-returned data in  to support scenarios where the caller's buffer is smaller than the available data.

The function validates the connection state to ensure it's in an appropriate COPY mode (COPY_OUT or COPY_BOTH), uses getCopyDataMessage to recognize and validate incoming messages, and manages buffer copying with support for partial reads when the caller's buffer is insufficient.

## Parameters / Member Variables
- `*conn`: PostgreSQL connection handle containing the input buffer and connection state
- `*buffer`: Caller-provided buffer to receive the COPY data
- `bufsize`: Size of the caller's buffer in bytes
## Dependencies
- Functions called/Symbols referenced:
  - [getCopyDataMessage](../g/getCopyDataMessage.md)
  - PGASYNC_COPY_OUT (connection status constant)
  - PGASYNC_COPY_BOTH (connection status constant)
- Called from (representative examples):
  - [PQgetlineAsync](../P/PQgetlineAsync.md) (in src/interfaces/libpq/fe-exec.c)

## Notes and Other Information
- Returns the number of bytes copied to the buffer, 0 if no data is available yet, or -1 for end-of-copy or error conditions
- Maintains state across calls using  to handle cases where the caller's buffer is smaller than the available message
- Does not change  unlike pqGetCopyData3, allowing PQendcopy to work without blocking
- Part of the libpq protocol 3 implementation for PostgreSQL client-server communication
- Designed for non-blocking I/O operations in asynchronous applications

## Simplified Source

```c
int pqGetlineAsync3(PGconn *conn, char *buffer, int bufsize) {
    int msgLength;
    int avail;

    // Verify we're in COPY mode
    if (conn->asyncStatus != PGASYNC_COPY_OUT &&
        conn->asyncStatus != PGASYNC_COPY_BOTH)
        return -1;  // Not doing a copy

    // Get the next copy data message (non-blocking)
    msgLength = getCopyDataMessage(conn);
    if (msgLength < 0)
        return -1;  // End of copy or error
    if (msgLength == 0)
        return 0;   // No data available yet

    // Calculate available data (account for already-returned data)
    conn->inCursor += conn->copy_already_done;
    avail = msgLength - 4 - conn->copy_already_done;  // Subtract message header

    if (avail <= bufsize) {
        // Copy entire remaining message
        memcpy(buffer, &conn->inBuffer[conn->inCursor], avail);

        // Mark message as fully consumed
        conn->inStart = conn->inCursor + avail;
        conn->copy_already_done = 0;  // Reset for next message
        return avail;
    } else {
        // Return partial message (buffer too small)
        memcpy(buffer, &conn->inBuffer[conn->inCursor], bufsize);

        // Track how much we've returned so far
        conn->copy_already_done += bufsize;
        return bufsize;
    }
}
```