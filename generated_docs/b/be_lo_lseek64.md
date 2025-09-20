# be_lo_lseek64

## Location
[src/backend/libpq/be-fsstubs.c:231-248](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-fsstubs.c#L231-L248)

## Overview
A PostgreSQL backend function that provides 64-bit seek functionality for large objects, allowing position changes within large object data streams.

## Definition

```c
Datum
be_lo_lseek64(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the backend support for PostgreSQL's lo_lseek64 large object function, which allows seeking to a specific position within a large object using 64-bit offsets. The function validates the provided file descriptor, performs the seek operation using the internal inv_seek function, and returns the new position as a 64-bit integer. This extends the capabilities beyond the 32-bit lo_lseek function by supporting larger file offsets.

## Parameters / Member Variables
-  (int32): Large object file descriptor (extracted from PG_GETARG_INT32(0))
-  (int64): 64-bit byte offset for seeking (extracted from PG_GETARG_INT64(1))  
-  (int32): Seek origin flag (SEEK_SET, SEEK_CUR, or SEEK_END) (extracted from PG_GETARG_INT32(2))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 (parameter extraction macro)
  - PG_GETARG_INT64 (64-bit parameter extraction macro)  
  - [inv_seek](../i/inv_seek.md) (internal large object seek function)
  - PG_RETURN_INT64 (64-bit return value macro)
  - ereport/ERROR (error reporting for invalid descriptors)
- Called from (representative examples):
  - No direct references found (likely called via PostgreSQL function call mechanism)

## Notes and Other Information
- Located in src/backend/libpq/be-fsstubs.c:231-248
- Validates file descriptor against cookies array before proceeding
- Supports 64-bit offsets, enabling operations on very large objects (>4GB)
- Returns the new absolute position within the large object
- Part of PostgreSQL's large object API for handling binary data