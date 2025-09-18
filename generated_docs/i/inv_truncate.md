# inv_truncate

## Location
src/backend/storage/large_object/inv_api.c: 778 - 953

## Overview
Truncates a PostgreSQL large object to a specified length, similar to ftruncate() in standard C file I/O, handling page-based storage by updating the target page and deleting all pages beyond the truncation point.

## Definition
```c
void inv_truncate(LargeObjectDesc *obj_desc, int64 len)
```

## Detailed Description
The `inv_truncate` function truncates a large object to the specified length by either shortening or lengthening it. This function is part of PostgreSQL's large object (BLOB) API and serves as the equivalent of the standard C library's `ftruncate()` function for large objects.

The function handles PostgreSQL's page-based storage system for large objects, where data is stored in chunks of LOBLKSIZE (typically 2KB). It efficiently manages three scenarios:
1. Truncating within an existing page (shortens the page data)
2. Truncating in a hole (creates a new page with zero-fill up to the truncation point)
3. Truncating beyond existing data (similar to hole case)

Key behaviors include:
- Permission checking to ensure write access (IFS_WRLOCK flag)
- Length validation (must be >= 0 and <= MAX_LARGE_OBJECT_SIZE)
- Efficient page-based storage management
- Proper handling of sparse large objects with holes
- Complete removal of pages beyond the truncation point
- Transaction-safe catalog updates with proper indexing
- Command counter increment for transaction visibility

The function works by:
1. Calculating the target page number and offset within that page
2. Scanning for existing pages starting from the target page
3. If target page exists: loading data, truncating at specified offset, updating tuple
4. If target page doesn't exist: creating new page with zero-fill up to truncation point
5. Deleting all pages beyond the truncation point
6. Maintaining proper catalog indexes and transaction visibility

## Parameters / Member Variables
- `obj_desc`: Pointer to a LargeObjectDesc structure representing an open large object. Must have write permissions (IFS_WRLOCK flag set).
- `len`: Target length for the large object in bytes. Must be >= 0 and <= MAX_LARGE_OBJECT_SIZE.

## Dependencies
- Functions called/Symbols referenced:
  - `PointerIsValid` (pointer validation macro)
  - `open_lo_relation`, `CatalogOpenIndexes`, `CatalogCloseIndexes` (catalog management)
  - `systable_beginscan_ordered`, `systable_getnext_ordered`, `systable_endscan_ordered` (system catalog scanning)
  - `getdatafield` (extracts data from large object tuple)
  - `heap_modify_tuple`, `heap_form_tuple`, `heap_freetuple` (tuple management)
  - `CatalogTupleUpdateWithInfo`, `CatalogTupleInsertWithInfo`, `CatalogTupleDelete` (catalog updates)
  - `CommandCounterIncrement` (transaction visibility)
  - `MemSet` (memory zeroing for holes)
  - `MAX_LARGE_OBJECT_SIZE`, `LOBLKSIZE` (size constants)
- Called from (representative examples):
  - `lo_truncate_internal` (user-facing truncate function)

## Notes and Other Information
- Returns void (no return value)
- Validates length parameter (0 <= len <= MAX_LARGE_OBJECT_SIZE)
- Requires IFS_WRLOCK permission flag to be set in the object descriptor
- Efficiently handles sparse large objects by creating minimal pages with zero-fill
- Properly deletes all pages beyond the truncation point to reclaim storage
- Uses ordered index scans and proper catalog management for consistency
- Maintains transaction safety with proper tuple versioning and command counter increments
- Handles both shortening (removing data) and lengthening (adding zero-filled holes) operations
- Memory management includes proper cleanup of temporary data structures
- Uses internal error messages for length validation to avoid exposing internal format strings
- The object's current position (offset) is not modified by this operation