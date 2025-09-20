# PTEntryArray

## Location
[src/backend/nodes/tidbitmap.c:112-116](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/tidbitmap.c#L112-L116)

## Overview
PTEntryArray is a reference-counted container structure that holds an array of PagetableEntry objects, designed for shared access across multiple TIDBitmap iterators in PostgreSQL's parallel processing system.

## Definition

```c
typedef struct PTEntryArray
{
	pg_atomic_uint32 refcount;	/* no. of iterator attached */
	PagetableEntry ptentry[FLEXIBLE_ARRAY_MEMBER];
} PTEntryArray;
```
## Detailed Description
PTEntryArray serves as a shared container for PagetableEntry objects in PostgreSQL's TIDBitmap system, specifically designed to support parallel bitmap iterations. The structure uses atomic reference counting to manage concurrent access from multiple iterator processes safely. The flexible array member allows the structure to accommodate varying numbers of page table entries without requiring separate memory allocations.

This design enables efficient memory sharing in parallel query execution where multiple worker processes need access to the same bitmap data. The atomic reference count ensures proper lifecycle management, preventing premature deallocation while iterators are still using the data.

## Parameters / Member Variables
- : Atomic reference counter tracking the number of iterators currently attached to this array, ensuring safe concurrent access and proper memory management
- : Flexible array member containing the actual PagetableEntry objects, sized dynamically based on the number of entries needed

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_uint32](../p/pg_atomic_uint32.md)
  - [PagetableEntry](PagetableEntry.md)
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - [tbm_free_shared_area](../t/tbm_free_shared_area.md)
  - [tbm_prepare_shared_iterate](../t/tbm_prepare_shared_iterate.md)
  - [pagetable_allocate](../p/pagetable_allocate.md)

## Notes and Other Information
- The atomic reference counting mechanism is crucial for thread-safe operations in PostgreSQL's parallel query execution environment
- The flexible array member design optimizes memory layout by storing PagetableEntry objects contiguously within the same allocation
- This structure is primarily used in shared memory contexts where multiple parallel workers need coordinated access to bitmap data
- The reference counting ensures that the array remains valid as long as any iterator is using it, preventing use-after-free errors in parallel execution scenarios