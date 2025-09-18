# lo_get_fragment_internal

## Location
src/backend/libpq/be-fsstubs.c: 741 - 791

## Overview
Reads a specified fragment of data from a large object, handling bounds checking, size validation, and returning the data as a bytea structure.

## Definition
```c
static bytea *lo_get_fragment_internal(Oid loOid, int64 offset, int32 nbytes)
```

## Detailed Description
lo_get_fragment_internal is a core utility function that implements safe reading of large object fragments. It provides comprehensive bounds checking and size validation:

1. Opens the large object in read-only mode using `inv_open`
2. Determines the large object's total size by seeking to the end
3. Calculates the actual read length by considering:
   - The requested byte count (nbytes), where -1 means read to end
   - The large object's boundaries to prevent reading beyond the end
   - The starting offset position
4. Validates that the result size won't exceed PostgreSQL's maximum allocation size
5. Allocates a bytea structure with appropriate header space
6. Seeks to the specified offset and reads the calculated amount of data
7. Sets the bytea size header and closes the large object
8. Returns the populated bytea structure

The function handles edge cases like reading beyond the object end, zero-length results, and oversized requests with appropriate error reporting.

## Parameters / Member Variables
- `loOid`: OID of the large object to read from
- `offset`: Starting position within the large object (0-based)
- `nbytes`: Number of bytes to read, or -1 to read to the end of the object

## Dependencies
- Functions called/Symbols referenced:
  - [inv_open](../i/inv_open.md)
  - [inv_seek](../i/inv_seek.md)
  - [inv_read](../i/inv_read.md)
  - [inv_close](../i/inv_close.md)
  - [palloc](../p/palloc.md)
  - ereport
  - [LargeObjectDesc](../L/LargeObjectDesc.md) (struct type)
  - INV_READ (constant)
  - MaxAllocSize (constant)
  - VARDATA (macro)
  - SET_VARSIZE (macro)
  - PG_USED_FOR_ASSERTS_ONLY (attribute)
- Called from (representative examples):
  - [be_lo_get](../b/be_lo_get.md) (src/backend/libpq/be-fsstubs.c:797)
  - [be_lo_get_fragment](../b/be_lo_get_fragment.md) (src/backend/libpq/be-fsstubs.c:818)

## Notes and Other Information
- Static function with file-local scope, used as a building block for public large object read operations
- Implements comprehensive bounds checking to prevent buffer overflows and invalid memory access
- Handles the special case of nbytes == -1 to read from offset to end of object
- Uses assertions to verify that the actual bytes read match the calculated expected length
- Allocates result using `palloc` in the current memory context
- Sets `lo_cleanup_needed` flag to ensure proper transaction cleanup
- Enforces PostgreSQL's maximum allocation size limit to prevent excessive memory usage
- Returns a properly formatted bytea with VARHDRSZ header for PostgreSQL's variable-length data types
- Opens and closes the large object within the same function call, ensuring resource cleanup even on errors