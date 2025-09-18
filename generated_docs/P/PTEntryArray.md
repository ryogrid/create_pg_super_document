# PTEntryArray

## Location
src/backend/nodes/tidbitmap.c: 112 - 116

## Overview
PTEntryArray is a reference-counted container structure that holds an array of PagetableEntry objects, designed for shared access across multiple TIDBitmap iterators in PostgreSQL's parallel processing system.

## Definition


## Detailed Description
PTEntryArray serves as a shared container for PagetableEntry objects in PostgreSQL's TIDBitmap system, specifically designed to support parallel bitmap iterations. The structure uses atomic reference counting to manage concurrent access from multiple iterator processes safely. The flexible array member allows the structure to accommodate varying numbers of page table entries without requiring separate memory allocations.

This design enables efficient memory sharing in parallel query execution where multiple worker processes need access to the same bitmap data. The atomic reference count ensures proper lifecycle management, preventing premature deallocation while iterators are still using the data.

## Parameters / Member Variables
- : Atomic reference counter tracking the number of iterators currently attached to this array, ensuring safe concurrent access and proper memory management
- : Flexible array member containing the actual PagetableEntry objects, sized dynamically based on the number of entries needed

## Dependencies
- Functions called/Symbols referenced:
  - pg_atomic_uint32
  - PagetableEntry
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - tbm_free_shared_area
  - tbm_prepare_shared_iterate
  - pagetable_allocate

## Notes and Other Information
- The atomic reference counting mechanism is crucial for thread-safe operations in PostgreSQL's parallel query execution environment
- The flexible array member design optimizes memory layout by storing PagetableEntry objects contiguously within the same allocation
- This structure is primarily used in shared memory contexts where multiple parallel workers need coordinated access to bitmap data
- The reference counting ensures that the array remains valid as long as any iterator is using it, preventing use-after-free errors in parallel execution scenarios