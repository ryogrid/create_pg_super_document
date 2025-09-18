# IndexFetchHeapData

## Location
src/include/access/heapam.h: 114 - 120

## Overview
IndexFetchHeapData is a structure that manages state information for fetching heap tuples via index lookups, extending the generic IndexFetchTableData with heap-specific buffer management.

## Definition
```c
typedef struct IndexFetchHeapData
{
    IndexFetchTableData xs_base;    /* AM independent part of the descriptor */
    Buffer              xs_cbuf;    /* current heap buffer in scan, if any */
    /* NB: if xs_cbuf is not InvalidBuffer, we hold a pin on that buffer */
} IndexFetchHeapData;
```

## Detailed Description
IndexFetchHeapData serves as the heap-specific implementation of index fetch operations, extending the generic IndexFetchTableData base class. This structure maintains the essential state for fetching heap tuples identified by index scans, particularly managing the current heap buffer to optimize performance by avoiding repeated buffer lookups when fetching multiple tuples from the same page. The structure ensures proper buffer pin management to prevent premature buffer eviction during ongoing fetch operations.

## Parameters / Member Variables
- `xs_base`: Generic index fetch table data containing the relation being accessed
- `xs_cbuf`: Buffer identifier for the current heap page being accessed during tuple fetches

## Dependencies
- Functions called/Symbols referenced:
  - [IndexFetchTableData](IndexFetchTableData.md)
  - Buffer
- Called from (representative examples):
  - [heapam_index_fetch_begin](../h/heapam_index_fetch_begin.md)
  - [heapam_index_fetch_reset](../h/heapam_index_fetch_reset.md)
  - [heapam_index_fetch_end](../h/heapam_index_fetch_end.md)
  - [heapam_index_fetch_tuple](../h/heapam_index_fetch_tuple.md)

## Notes and Other Information
- When xs_cbuf is not InvalidBuffer, a pin is held on that buffer to prevent eviction
- Used specifically for index-guided heap tuple fetches in the heap access method
- Part of the table AM (Access Method) interface for index operations
- Optimizes performance by caching the current heap buffer across multiple tuple fetches
- The structure is allocated and managed by heap AM handler functions
- Essential for index scan operations that need to fetch actual tuple data from heap pages
- Extends the generic IndexFetchTableData to provide heap-specific functionality