# xl_heap_inplace

## Location
src/include/access/heapam_xlog.h: 425 - 428

## Overview
A WAL record structure used to log in-place update operations on tuples in PostgreSQL heap tables, capturing minimal information for non-MVCC updates.

## Definition
```c
typedef struct xl_heap_inplace
{
    OffsetNumber offnum;        /* updated tuple's offset on page */
} xl_heap_inplace;
```

## Detailed Description
The `xl_heap_inplace` structure is a WAL record format used to log in-place update operations in PostgreSQL's heap access method. In-place updates are a special type of tuple modification that directly overwrites the existing tuple data without creating a new tuple version, bypassing the normal MVCC (Multi-Version Concurrency Control) mechanism.

This type of update is used in specific scenarios where MVCC versioning is not required or desirable, such as updating system catalog tuples or in certain optimization cases where the update doesn't affect user-visible data semantics. The WAL record is minimal, containing only the offset of the updated tuple, as the actual tuple data changes are logged separately.

## Parameters / Member Variables
- `offnum`: The offset number of the tuple being updated in-place within its heap page

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references)
- Called from (representative examples):
  - [heap_inplace_update_and_unlock](../h/heap_inplace_update_and_unlock.md)
  - [heap_inplace_update](../h/heap_inplace_update.md)
  - [heap_xlog_inplace](../h/heap_xlog_inplace.md)
  - [heap_desc](../h/heap_desc.md)
  - SizeOfHeapInplace

## Notes and Other Information
- Used for specialized update operations that bypass normal MVCC mechanisms
- Typically employed for system catalog updates or internal optimizations
- Minimal WAL record structure reflecting the straightforward nature of in-place operations
- Critical for crash recovery to properly replay in-place modifications
- Does not create new tuple versions, unlike standard heap updates
- Must be used carefully as it can violate MVCC semantics if applied inappropriately