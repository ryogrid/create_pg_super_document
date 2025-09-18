# be_lo_tell

## Location
src/backend/libpq/be-fsstubs.c: 275 - 297

## Overview
A PostgreSQL backend function that returns the current read/write position within a large object as a 32-bit integer.

## Definition
```c
Datum be_lo_tell(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the backend support for PostgreSQL's lo_tell() large object function, which returns the current position (offset) within a large object. The function validates the file descriptor, retrieves the current position using inv_tell, and includes overflow protection to ensure the 64-bit internal position can be safely represented as a 32-bit value. If the position exceeds the 32-bit range, an error is reported.

## Parameters / Member Variables
- `fd` (int32): Large object file descriptor (extracted from PG_GETARG_INT32(0))
- `offset` (int64): Internal variable to store the 64-bit position from inv_tell
- Returns: Current position as int32 (with overflow checking)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 (parameter extraction macro)
  - inv_tell (internal large object position query function)
  - PG_RETURN_INT32 (32-bit return value macro)
  - ereport/ERROR (error reporting for invalid descriptors and overflow)
  - errcode (error code functions for specific error types)
- Called from (representative examples):
  - No direct references found (likely called via PostgreSQL function call mechanism)

## Notes and Other Information
- Located in src/backend/libpq/be-fsstubs.c:275-297
- Validates file descriptor against cookies array before proceeding
- Includes overflow protection - reports error if position exceeds int32 range
- For positions beyond 32-bit range, use be_lo_tell64 instead
- Part of PostgreSQL's large object API for position tracking
- The overflow check ensures backward compatibility with 32-bit interfaces
- Returns current absolute position within the large object data stream