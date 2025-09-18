# lo_write

## Location
src/interfaces/libpq/fe-lobj.c: 295 - 343

## Overview
Writes data to a large object using a file descriptor obtained from lo_open or lo_creat.

## Definition
```c
int lo_write(int fd, const char *buf, int len)
```

## Detailed Description
The lo_write function is the backend implementation for writing data to a PostgreSQL large object. It validates the file descriptor, ensures the large object was opened with write permissions, and then delegates the actual writing operation to the lower-level inv_write function. This function is part of PostgreSQL's large object API that provides file-like operations for binary large objects stored in the database.

## Parameters / Member Variables
- `fd`: Large object file descriptor obtained from lo_open or lo_creat
- `buf`: Pointer to the data buffer to write
- `len`: Number of bytes to write from the buffer

## Dependencies
- Functions called/Symbols referenced:
  - inv_write
  - LargeObjectDesc
- Called from (representative examples):
  - be_lowrite
  - dump_lo_buf
  - lo_import_internal
  - importFile (test examples)

## Notes and Other Information
- Returns the number of bytes actually written, or -1 on error
- Validates that the file descriptor is valid and the large object was opened for writing
- Part of PostgreSQL's fastpath interface for large objects
- Located in src/backend/libpq/be-fsstubs.c:182-205
- Requires the large object to have been opened with write lock (IFS_WRLOCK flag)