# pqGetline3

## Location
src/interfaces/libpq/fe-protocol3.c: 1810 - 1860

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
  - PQgetlineAsync
  - pqWait
  - pqReadData
  - libpq_append_conn_error
  - strcpy
  - PGINVALID_SOCKET, PGASYNC_COPY_OUT, PGASYNC_COPY_BOTH
- Called from (representative examples):
  - PQgetline (public API wrapper)

## Notes and Other Information
- Returns: 0 if line successfully read, 1 if buffer filled without finding newline, EOF on error or end of copy
- Only works with text-format COPY operations (rejects binary format)
- Automatically strips trailing newline characters from returned lines
- Generates "\\\." terminator string when end of copy is reached (legacy compatibility)
- Provides blocking behavior - will not return until a complete line is available or error occurs
- Validates connection state before attempting to read data
- Part of the legacy line-oriented COPY interface, largely superseded by PQgetCopyData