# pqGetline3

## Location
[src/interfaces/libpq/fe-protocol3.c:1810-1860](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-protocol3.c#L1810-L1860)

## Overview
Gets a newline-terminated string from the PostgreSQL backend during text-format COPY OUT operations, implementing the protocol 3.0 version with blocking behavior.

## Definition
```c
int pqGetline3(PGconn *conn, char *s, int maxlen)
```

## Detailed Description
The pqGetline3 function provides a synchronous, blocking interface for reading text lines during COPY OUT operations. It is designed to maintain compatibility with the traditional line-oriented COPY interface while using the more robust protocol 3.0 infrastructure. The function validates that the connection is in the correct state for text COPY operations, then uses PQgetlineAsync in a loop with network I/O blocking to ensure a complete line is read.

The function handles the legacy line-oriented COPY protocol semantics, including automatic newline stripping and the generation of the traditional "\\\." end-of-copy terminator. It provides a simplified interface compared to the more flexible pqGetCopyData3 function, specifically tailored for applications that expect line-by-line text processing.

## Parameters / Member Variables
- `conn`: PostgreSQL connection object that must be in COPY OUT state
- `s`: Buffer to store the retrieved line (must be allocated by caller)
- `maxlen`: Maximum length of the buffer including space for null terminator

## Dependencies
- Functions called/Symbols referenced:
  - [PQgetlineAsync](../P/PQgetlineAsync.md)
  - [pqWait](pqWait.md)
  - [pqReadData](pqReadData.md)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md)
  - strcpy
  - PGINVALID_SOCKET, PGASYNC_COPY_OUT, PGASYNC_COPY_BOTH
- Called from (representative examples):
  - [PQgetline](../P/PQgetline.md) (public API wrapper)

## Notes and Other Information
- Returns: 0 if line successfully read, 1 if buffer filled without finding newline, EOF on error or end of copy
- Only works with text-format COPY operations (rejects binary format)
- Automatically strips trailing newline characters from returned lines
- Generates "\\\." terminator string when end of copy is reached (legacy compatibility)
- Provides blocking behavior - will not return until a complete line is available or error occurs
- Validates connection state before attempting to read data
- Part of the legacy line-oriented COPY interface, largely superseded by PQgetCopyData

## Simplified Source

```c
int pqGetline3(PGconn *conn, char *s, int maxlen) {
    int status;

    // Validate connection state for text COPY OUT
    if (conn->sock == PGINVALID_SOCKET ||
        (conn->asyncStatus != PGASYNC_COPY_OUT &&
         conn->asyncStatus != PGASYNC_COPY_BOTH) ||
        conn->copy_is_binary) {
        libpq_append_conn_error(conn, "PQgetline: not doing text COPY OUT");
        *s = '\0';
        return EOF;
    }

    // Keep trying to get a line until we succeed
    while ((status = PQgetlineAsync(conn, s, maxlen - 1)) == 0) {
        // Wait for more data from network
        if (pqWait(true, false, conn) || pqReadData(conn) < 0) {
            *s = '\0';
            return EOF;  // Network error
        }
    }

    if (status < 0) {
        // End of copy - generate legacy terminator
        strcpy(s, "\\\\.");
        return 0;
    }

    // Process the line: strip newline and null-terminate
    if (s[status - 1] == '\n') {
        s[status - 1] = '\0';
        return 0;  // Complete line
    } else {
        s[status] = '\0';
        return 1;  // Buffer filled without newline
    }
}
```