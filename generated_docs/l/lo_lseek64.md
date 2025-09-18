# lo_lseek64

## Location
src/interfaces/libpq/fe-lobj.c: 385 - 437

## Overview
Changes the current read or write position within a large object using a 64-bit offset, supporting large objects larger than 2GB.

## Definition
```c
pg_int64 lo_lseek64(PGconn *conn, int fd, pg_int64 offset, int whence)
```

## Detailed Description
The lo_lseek64 function is the client-side implementation for seeking within a PostgreSQL large object using 64-bit offsets. It extends the capabilities of lo_lseek by supporting large objects up to the theoretical limit of PostgreSQL's large object implementation. The function handles network byte order conversion for the 64-bit offset and communicates with the backend via the fastpath interface.

## Parameters / Member Variables
- `conn`: PostgreSQL database connection handle
- `fd`: Large object file descriptor obtained from lo_open or lo_creat
- `offset`: 64-bit number of bytes to move the position (can be negative)
- `whence`: Position reference point (SEEK_SET, SEEK_CUR, or SEEK_END)

## Dependencies
- Functions called/Symbols referenced:
  - [lo_initialize](lo_initialize.md)
  - [lo_hton64](lo_hton64.md)
  - [lo_ntoh64](lo_ntoh64.md)
  - PQfn
  - PQArgBlock
  - PGRES_COMMAND_OK
  - pg_int64
- Called from (representative examples):
  - [pickout](../p/pickout.md) (test example in testlo64.c)
  - [overwrite](../o/overwrite.md) (test example in testlo64.c)

## Notes and Other Information
- Returns the new 64-bit position within the large object, or -1 on error
- Uses pg_int64 for 64-bit offset support, enabling large objects > 2GB
- Part of PostgreSQL's client-side large object interface (libpq)
- Located in src/interfaces/libpq/fe-lobj.c:385-437
- Requires backend support for lo_lseek64 function (checks fn_lo_lseek64 != 0)
- Handles network byte order conversion with lo_hton64/lo_ntoh64
- Validates result length is 8 bytes to ensure proper 64-bit return value