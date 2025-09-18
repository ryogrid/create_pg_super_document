# tbm_prepare_shared_iterate

## Location
[src/backend/nodes/tidbitmap.c:766-910](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/tidbitmap.c#L766-L910)

## Overview
Prepares shared iteration state for a TIDBitmap that can be accessed by multiple processes using dynamic shared memory.

## Definition
```c
dsa_pointer tbm_prepare_shared_iterate(TIDBitmap *tbm)
```

## Detailed Description
This function sets up shared iteration infrastructure for parallel bitmap scans across multiple processes. It allocates a TBMSharedIteratorState structure in dynamic shared memory (DSA) and converts the bitmap data into a format suitable for shared access. The function handles both TBM_HASH and TBM_ONE_PAGE modes, creating shared arrays of page and chunk indices that multiple processes can access concurrently. It uses reference counting to manage the lifecycle of shared resources and initializes a lightweight lock for coordinating access among parallel workers. The conversion from hash table to index arrays enables efficient parallel iteration while maintaining sorted order.

## Parameters / Member Variables
- `tbm`: Pointer to the TIDBitmap structure to be prepared for shared iteration

## Dependencies
- Functions called/Symbols referenced:
  - [TBMSharedIteratorState](../T/TBMSharedIteratorState.md) (structure type)
  - [PTEntryArray](../P/PTEntryArray.md) (structure type)
  - [PTIterationArray](../P/PTIterationArray.md) (structure type)
  - TBM_ITERATING_PRIVATE (constant)
  - dsa_allocate0 (function)
  - [dsa_get_address](../d/dsa_get_address.md) (function)
  - TBM_NOT_ITERATING (constant)
  - [PagetableEntry](../P/PagetableEntry.md) (structure type)
  - dsa_allocate (function)
  - [pg_atomic_init_u32](../p/pg_atomic_init_u32.md) (function)
  - TBM_HASH (constant)
  - TBM_ONE_PAGE (constant)
  - qsort_arg (function)
  - [tbm_shared_comparator](tbm_shared_comparator.md) (function)
  - [pg_atomic_add_fetch_u32](../p/pg_atomic_add_fetch_u32.md) (function)
  - LWLockInitialize (function)
  - LWTRANCHE_SHARED_TIDBITMAP (constant)
  - TBM_ITERATING_SHARED (constant)
- Called from (representative examples):
  - [BitmapHeapNext](../B/BitmapHeapNext.md) (src/backend/executor/nodeBitmapHeapscan.c:148, 153)

## Notes and Other Information
- Returns a dsa_pointer that can be shared across processes for parallel iteration
- Converts pagetable hash entries into sorted index arrays stored in dynamic shared memory
- Uses reference counting to manage shared resource lifecycle across multiple processes
- Initializes a lightweight lock (LWTRANCHE_SHARED_TIDBITMAP) for coordinating shared access
- Handles both hash table (TBM_HASH) and single page (TBM_ONE_PAGE) bitmap modes
- The shared state includes page and chunk arrays with atomic reference counters
- Essential for parallel bitmap heap scans where multiple workers need to coordinate access