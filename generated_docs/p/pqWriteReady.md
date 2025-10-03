# pqWriteReady

## Location
[src/interfaces/libpq/fe-misc.c:1053-1066](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-misc.c#L1053-L1066)

## Overview
pqWriteReady is a PostgreSQL libpq function that checks if the connection socket is immediately ready for writing without blocking.

## Definition

```c
int
pqWriteReady(PGconn *conn)
```
## Detailed Description
pqWriteReady provides a non-blocking check to determine if the connection socket is available for writing data. It uses pqSocketCheck with parameters configured for write readiness checking and immediate return (no timeout). This function is useful when the caller wants to know if a write operation would succeed without blocking the thread.

## Parameters / Member Variables
- `*conn`: Pointer to the PGconn connection structure representing the database connection
## Dependencies
- Functions called/Symbols referenced:
  - [pqSocketCheck](pqSocketCheck.md)
- Called from (representative examples):
  - pgunlock_thread

## Notes and Other Information
- Returns -1 on failure, 0 if not ready for writing, 1 if ready for writing
- This is a non-blocking operation (immediate return) that uses a timeout of 0
- The function checks only for write readiness, not read readiness
- Internally calls pqSocketCheck(conn, 0, 1, 0) where parameters are (conn, forRead=0, forWrite=1, timeout=0)
- File location: src/interfaces/libpq/fe-misc.c:1053-1066