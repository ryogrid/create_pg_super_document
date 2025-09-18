# be_lo_close

## Location
src/backend/libpq/be-fsstubs.c: 126 - 153

## Overview
Closes a previously opened PostgreSQL large object by its file descriptor and performs necessary cleanup operations.

## Definition
```c
Datum be_lo_close(PG_FUNCTION_ARGS)
```

## Detailed Description
The `be_lo_close` function closes a large object that was previously opened with `be_lo_open`. It validates the provided file descriptor, ensures it corresponds to a valid open large object, and then performs the necessary cleanup operations to release resources associated with the large object descriptor. The function returns 0 on successful closure.

## Parameters / Member Variables
- `PG_GETARG_INT32(0)`: File descriptor of the large object to close

## Dependencies
- Functions called/Symbols referenced:
  - DEBUG4
  - [closeLOfd](../c/closeLOfd.md)
- Called from (representative examples):
  - No direct references found (likely called via PostgreSQL function call mechanism)

## Notes and Other Information
- Validates file descriptor bounds and ensures it references an open large object
- Reports ERROR with ERRCODE_UNDEFINED_OBJECT for invalid file descriptors
- Includes debug logging when FSDB is defined
- Always returns 0 (success) after successful closure
- Part of PostgreSQL's large object API accessible via SQL functions