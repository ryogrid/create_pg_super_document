# be_lo_tell64

## Location
[src/backend/libpq/be-fsstubs.c:298-313](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-fsstubs.c#L298-L313)

## Overview
A PostgreSQL backend function that returns the current read/write position within a large object as a 64-bit integer, supporting very large file positions.

## Definition
```c
Datum be_lo_tell64(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the backend support for PostgreSQL's lo_tell64() large object function, which returns the current position (offset) within a large object using 64-bit precision. Unlike be_lo_tell, this function can handle positions beyond the 32-bit range without overflow errors, making it suitable for very large objects (>4GB). The function validates the file descriptor and directly returns the full 64-bit position from inv_tell.

## Parameters / Member Variables
- `fd` (int32): Large object file descriptor (extracted from PG_GETARG_INT32(0))
- `offset` (int64): Variable to store the 64-bit position from inv_tell
- Returns: Current position as int64 (full 64-bit range supported)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 (parameter extraction macro)
  - [inv_tell](../i/inv_tell.md) (internal large object position query function)
  - PG_RETURN_INT64 (64-bit return value macro)
  - ereport/ERROR (error reporting for invalid descriptors)
  - [errcode](../e/errcode.md) (error code functions for specific error types)
- Called from (representative examples):
  - No direct references found (likely called via PostgreSQL function call mechanism)

## Notes and Other Information
- Located in src/backend/libpq/be-fsstubs.c:298-313
- Validates file descriptor against cookies array before proceeding
- No overflow checking needed since both internal position and return value are 64-bit
- Complements be_lo_lseek64 for full 64-bit large object positioning support
- Part of PostgreSQL's large object API for handling very large binary data
- Suitable for large objects exceeding 4GB in size
- Returns current absolute position within the large object data stream
- Simpler implementation than be_lo_tell due to no overflow concerns