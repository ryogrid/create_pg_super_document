# inv_tell

## Location
[src/backend/storage/large_object/inv_api.c:475-487](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/large_object/inv_api.c#L475-L487)

## Overview
Returns the current read/write position (offset) within a PostgreSQL large object, similar to ftell() in standard C file I/O.

## Definition
```c
int64 inv_tell(LargeObjectDesc *obj_desc)
```

## Detailed Description
The `inv_tell` function provides the current position indicator for a large object descriptor. It returns the byte offset from the beginning of the large object where the next read or write operation will occur. This function is part of PostgreSQL's large object (BLOB) API and serves as the equivalent of the standard C library's `ftell()` function for large objects.

The function performs no permission checks since both read and write permissions allow querying the current position. It simply returns the stored offset value from the large object descriptor structure.

## Parameters / Member Variables
- `obj_desc`: Pointer to a LargeObjectDesc structure representing an open large object. Must be a valid pointer to an initialized large object descriptor.

## Dependencies
- Functions called/Symbols referenced:
  - `PointerIsValid` (assertion macro to validate pointer)
  - [LargeObjectDesc](../L/LargeObjectDesc.md) (structure type for large object descriptors)
- Called from (representative examples):
  - [be_lo_tell](../b/be_lo_tell.md) (backend function for 32-bit tell operations)
  - [be_lo_tell64](../b/be_lo_tell64.md) (backend function for 64-bit tell operations)

## Notes and Other Information
- Returns an `int64` value representing the current byte position
- No permission checks are performed - both read and write access allow position queries
- The function includes an assertion to ensure the object descriptor pointer is valid
- This is a low-level function typically called through higher-level backend interfaces
- The returned offset can be used with `inv_seek` to restore a position later