# inv_write

## Location
src/backend/storage/large_object/inv_api.c: 581 - 777

## Overview
Writes data to a PostgreSQL large object starting at the current position, similar to fwrite() in standard C file I/O, handling page-based storage, size limits, and both updating existing pages and creating new ones.

## Definition
```c
int inv_write(LargeObjectDesc *obj_desc, const char *buf, int nbytes)
```

## Detailed Description
The `inv_write` function writes up to `nbytes` bytes from a buffer to a large object, starting from the current write position stored in the large object descriptor. This function is part of PostgreSQL's large object (BLOB) API and serves as the equivalent of the standard C library's `fwrite()` function for large objects.

The function handles PostgreSQL's page-based storage system for large objects, where data is stored in chunks of LOBLKSIZE (typically 2KB). It efficiently manages both updating existing pages and creating new pages as needed. The function enforces size limits and maintains proper transaction semantics.

Key behaviors include:
- Permission checking to ensure write access (IFS_WRLOCK flag)
- Size limit enforcement (MAX_LARGE_OBJECT_SIZE)
- Efficient page-based storage management
- Handling of sparse writes with zero-filled holes
- Transaction-safe catalog updates with proper indexing
- Command counter increment for transaction visibility

The function works by:
1. Scanning existing pages starting from the target page
2. For existing pages: loading current data, filling holes, merging new data, updating tuple
3. For new pages: zero-filling holes, inserting new data, creating new tuple
4. Maintaining proper catalog indexes and transaction visibility

## Parameters / Member Variables
- `obj_desc`: Pointer to a LargeObjectDesc structure representing an open large object. Must have write permissions (IFS_WRLOCK flag set).
- `buf`: Buffer containing the data to write. Must be at least `nbytes` bytes long.
- `nbytes`: Number of bytes to write to the large object.

## Dependencies
- Functions called/Symbols referenced:
  - `PointerIsValid` (pointer validation macro)
  - `[open_lo_relation](../o/open_lo_relation.md)`, `CatalogOpenIndexes`, `CatalogCloseIndexes` (catalog management)
  - `[systable_beginscan_ordered](../s/systable_beginscan_ordered.md)`, `systable_getnext_ordered`, `systable_endscan_ordered` (system catalog scanning)
  - `[getdatafield](../g/getdatafield.md)` (extracts data from large object tuple)
  - `[heap_modify_tuple](../h/heap_modify_tuple.md)`, `heap_form_tuple`, `heap_freetuple` (tuple management)
  - `[CatalogTupleUpdateWithInfo](../C/CatalogTupleUpdateWithInfo.md)`, `CatalogTupleInsertWithInfo` (catalog updates)
  - `CommandCounterIncrement` (transaction visibility)
  - `MemSet` (memory zeroing for holes)
  - `MAX_LARGE_OBJECT_SIZE`, `LOBLKSIZE` (size constants)
- Called from (representative examples):
  - `[lo_write](../l/lo_write.md)` (user-facing write function)
  - `[lo_import_internal](../l/lo_import_internal.md)` (large object import functionality)
  - `[be_lo_from_bytea](../b/be_lo_from_bytea.md)` (bytea to large object conversion)
  - `[be_lo_put](../b/be_lo_put.md)` (large object data replacement)

## Notes and Other Information
- Returns the actual number of bytes written (may be less than requested due to size limits)
- Returns 0 if nbytes <= 0
- Enforces MAX_LARGE_OBJECT_SIZE limit and raises error if exceeded
- Automatically handles sparse writes by zero-filling holes in pages
- Updates the object descriptor's offset to reflect the new position after writing
- Requires IFS_WRLOCK permission flag to be set in the object descriptor
- Uses ordered index scans and proper catalog management for consistency
- Maintains transaction safety with proper tuple versioning and command counter increments
- Efficiently handles both partial page updates and full page writes
- Memory management includes proper cleanup of temporary data structures