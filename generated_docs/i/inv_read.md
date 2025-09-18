# inv_read

## Location
src/backend/storage/large_object/inv_api.c: 488 - 580

## Overview
Reads data from a PostgreSQL large object starting at the current position, similar to fread() in standard C file I/O, handling page-based storage and potential gaps (holes) in the large object.

## Definition
```c
int inv_read(LargeObjectDesc *obj_desc, char *buf, int nbytes)
```

## Detailed Description
The `inv_read` function reads up to `nbytes` bytes from a large object into a buffer, starting from the current read position stored in the large object descriptor. This function is part of PostgreSQL's large object (BLOB) API and serves as the equivalent of the standard C library's `fread()` function for large objects.

The function handles PostgreSQL's page-based storage system for large objects, where data is stored in chunks of LOBLKSIZE (typically 2KB). It efficiently handles sparse large objects by detecting missing pages ("holes") and filling them with zeros. The function uses a system catalog scan to read pages in order, starting from the page containing the current offset.

Key behaviors include:
- Permission checking to ensure read access
- Handling of missing pages as zero-filled holes
- Sequential reading across multiple pages
- Automatic position tracking in the object descriptor

## Parameters / Member Variables
- `obj_desc`: Pointer to a LargeObjectDesc structure representing an open large object. Must have read permissions (IFS_RDLOCK flag set).
- `buf`: Buffer to store the read data. Must be at least `nbytes` bytes long.
- `nbytes`: Maximum number of bytes to read from the large object.

## Dependencies
- Functions called/Symbols referenced:
  - `PointerIsValid` (pointer validation macro)
  - [open_lo_relation](../o/open_lo_relation.md) (opens large object system relation)
  - [systable_beginscan_ordered](../s/systable_beginscan_ordered.md), `systable_getnext_ordered`, `systable_endscan_ordered` (system catalog scanning)
  - [getdatafield](../g/getdatafield.md) (extracts data from large object tuple)
  - `MemSet` (memory zeroing for holes)
  - `LOBLKSIZE` (large object block size constant)
- Called from (representative examples):
  - [lo_read](../l/lo_read.md) (user-facing read function)
  - [be_lo_export](../b/be_lo_export.md) (large object export functionality)
  - [lo_get_fragment_internal](../l/lo_get_fragment_internal.md) (internal fragment retrieval)

## Notes and Other Information
- Returns the actual number of bytes read (may be less than requested)
- Returns 0 if nbytes <= 0 or no data available
- Automatically handles sparse large objects by zero-filling missing pages
- Updates the object descriptor's offset to reflect the new position after reading
- Requires IFS_RDLOCK permission flag to be set in the object descriptor
- Uses ordered index scans for efficient sequential access to large object pages
- Handles variable-length data fields with proper memory management