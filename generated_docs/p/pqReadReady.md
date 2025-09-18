# pqReadReady

## Location
src/interfaces/libpq/fe-misc.c: 1043 - 1052

## Overview
pqReadReady is a PostgreSQL libpq function that checks if the connection socket is immediately ready for reading without blocking.

## Definition


## Detailed Description
pqReadReady provides a non-blocking check to determine if data is available for reading on the connection socket. It uses pqSocketCheck with parameters configured for read readiness checking and immediate return (no timeout). This function is useful when the caller wants to know if a read operation would succeed without blocking the thread.

## Parameters / Member Variables
- : Pointer to the PGconn connection structure representing the database connection

## Dependencies
- Functions called/Symbols referenced:
  - [pqSocketCheck](pqSocketCheck.md)
- Called from (representative examples):
  - [pqReadData](pqReadData.md)
  - [gss_read](../g/gss_read.md)

## Notes and Other Information
- Returns -1 on failure, 0 if not ready for reading, 1 if ready for reading
- This is a non-blocking operation (immediate return) that uses a timeout of 0
- The function checks only for read readiness, not write readiness
- Internally calls pqSocketCheck(conn, 1, 0, 0) where parameters are (conn, forRead=1, forWrite=0, timeout=0)
- File location: src/interfaces/libpq/fe-misc.c:1043-1052