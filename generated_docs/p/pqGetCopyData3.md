# pqGetCopyData3

## Location
src/interfaces/libpq/fe-protocol3.c: 1751 - 1809

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
  - getCopyDataMessage
  - pqWait
  - pqReadData
  - malloc
  - memcpy
  - libpq_append_conn_error
- Called from (representative examples):
  - PQgetCopyData (public API wrapper)

## Notes and Other Information
- Returns: row length (> 0) on success, 0 if no data available (async mode only), -1 if end of copy, -2 if error
- Caller is responsible for freeing the malloc'd buffer returned via the buffer parameter
- Automatically adds null terminator to returned data for convenience
- Handles zero-length messages by dropping them and continuing
- Supports both blocking and non-blocking operation modes
- Implements proper memory management and error reporting
- Critical component of PostgreSQL's COPY protocol implementation in libpq