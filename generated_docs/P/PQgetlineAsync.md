# PQgetlineAsync

## Location
[src/interfaces/libpq/fe-exec.c:2901-2917](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L2901-L2917)

## Overview
Asynchronous version of PQgetline that retrieves COPY data rows without blocking, designed for event-driven applications performing COPY TO STDOUT operations.

## Definition

```c
int
PQgetlineAsync(PGconn *conn, char *buffer, int bufsize)
```
## Detailed Description
PQgetlineAsync provides non-blocking access to COPY data during COPY OUT operations. Unlike PQgetline, this function is designed for asynchronous applications that cannot afford to block waiting for data. It automatically handles end-of-data detection and works with libpq's input buffer management system.

The function operates in conjunction with PQconsumeInput() to process incoming data as it becomes available. It returns complete data rows when possible, but can return partial rows if the provided buffer is smaller than the row size. The function takes full responsibility for detecting the end-of-copy signal, unlike the legacy PQgetline.

Key characteristics:
- Non-blocking operation suitable for event-driven applications
- Automatic end-of-data detection and handling
- Returns complete rows when possible, partial rows when buffer is too small
- Works with libpq's asynchronous input processing
- Data returned is not null-terminated (caller must handle this)

## Parameters / Member Variables
- : PostgreSQL connection handle that must be in a COPY OUT state
- : Pre-allocated buffer to receive the data
- : Size of the provided buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [pqGetlineAsync3](../p/pqGetlineAsync3.md)
- Called from (representative examples):
  - [pqGetline3](../p/pqGetline3.md) (internal fallback)
  - Event-driven applications using async COPY operations

## Notes and Other Information
- Returns -1 when end-of-copy marker is detected (caller must call PQendcopy)
- Returns 0 when no data is currently available (non-blocking behavior)
- Returns >0 indicating the number of bytes actually read
- Data does not extend beyond row boundaries
- Returned data is NOT null-terminated
- In text mode, complete rows end with '\n'
- Requires PQconsumeInput() calls to process incoming data
- Designed for applications that cannot block on I/O operations
- Must be used in conjunction with PQendcopy when end-of-data is detected
- More suitable than PQgetline for modern asynchronous applications