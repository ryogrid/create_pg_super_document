# lo_lseek

## Location
src/interfaces/libpq/fe-lobj.c: 344 - 384

## Overview
Changes the current read or write position within a large object using a 32-bit offset.

## Definition
```c
int lo_lseek(PGconn *conn, int fd, int offset, int whence)
```

## Detailed Description
The lo_lseek function is the client-side implementation for seeking within a PostgreSQL large object. It changes the current read or write position by calling the backend lo_lseek function via the fastpath interface. This function uses 32-bit integer offsets, making it suitable for large objects smaller than 2GB. For larger objects, use lo_lseek64 instead.

## Parameters / Member Variables
- `conn`: PostgreSQL database connection handle
- `fd`: Large object file descriptor obtained from lo_open or lo_creat
- `offset`: Number of bytes to move the position (can be negative)
- `whence`: Position reference point (SEEK_SET, SEEK_CUR, or SEEK_END)

## Dependencies
- Functions called/Symbols referenced:
  - lo_initialize
  - PQfn
  - PQArgBlock
  - PGRES_COMMAND_OK
- Called from (representative examples):
  - pickout (test example)
  - overwrite (test example)

## Notes and Other Information
- Returns the new position within the large object, or -1 on error
- Uses 32-bit signed integer for offset, limiting range to ±2GB
- Part of PostgreSQL's client-side large object interface (libpq)
- Located in src/interfaces/libpq/fe-lobj.c:344-384
- Communicates with backend via fastpath function calls
- whence parameter follows standard C library semantics (SEEK_SET=0, SEEK_CUR=1, SEEK_END=2)