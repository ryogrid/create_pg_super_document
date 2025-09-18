# heap_index_delete_tuples

## Location
src/backend/access/heap/heapam.c: 8095 - 8403

## Overview
Heapam implementation of tableam's index_delete_tuples interface that efficiently deletes multiple index tuples by examining their corresponding heap tuples and determining which are safe to delete.

## Definition
```c
TransactionId heap_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
```

## Detailed Description
This function is the core implementation of bulk index tuple deletion for heap tables. It serves as the heapam-specific implementation of the tableam interface, handling both simple index deletion and bottom-up index deletion operations.

The function performs several key operations:

1. **Sorting and Optimization**: Sorts the deltids array by TID and, for bottom-up deletion, reorders them to prioritize blocks with the most promising deletion candidates

2. **Prefetching**: Uses buffer prefetching to minimize I/O latency when accessing multiple heap pages, with prefetch distance determined by maintenance_io_concurrency settings

3. **Tuple Validation**: For each heap tuple referenced by index entries:
   - Validates the TID for corruption detection
   - Checks if the entire HOT chain is vacuumable using heap_hot_search_buffer
   - Determines if the tuple can be safely deleted

4. **Conflict Horizon Management**: Maintains a snapshotConflictHorizon by examining tuple headers throughout HOT chains to ensure recovery conflicts are properly handled

5. **Bottom-up Optimization**: For bottom-up deletion operations, implements space-based termination logic that stops processing when sufficient space has been freed

The function handles HOT (Heap-Only Tuples) chains by traversing from the index-referenced tuple through the entire chain, examining each tuple's visibility and updating the conflict horizon accordingly.

## Parameters / Member Variables
- `rel`: The heap relation containing the tuples to be deleted
- `delstate`: TM_IndexDeleteOp structure containing:
  - `deltids`: Array of TM_IndexDelete entries (TIDs to potentially delete)
  - `status`: Array of TM_IndexStatus entries tracking deletion status
  - `ndeltids`: Number of entries in deltids array
  - `bottomup`: Boolean indicating if this is a bottom-up deletion
  - `bottomupfreespace`: Target free space for bottom-up operations

## Dependencies
- Functions called/Symbols referenced:
  - index_delete_sort
  - bottomup_sort_and_shrink
  - index_delete_prefetch_buffer
  - index_delete_check_htid
  - heap_hot_search_buffer
  - HeapTupleHeaderAdvanceConflictHorizon
  - InitNonVacuumableSnapshot
  - ReadBuffer / UnlockReleaseBuffer
  - PageGetItemId / PageGetItem
  - ItemPointerGetBlockNumber / ItemPointerGetOffsetNumber
  - HeapTupleHeaderGetXmin / HeapTupleHeaderGetUpdateXid
  - IsCatalogRelation
  - get_tablespace_maintenance_io_concurrency
- Called from (representative examples):
  - Index AM implementations via tableam interface

## Notes and Other Information
- Supports two deletion modes: simple deletion and bottom-up deletion (space-driven optimization)
- Uses sophisticated prefetching strategy to minimize I/O costs when processing hundreds of tuples
- Implements comprehensive corruption detection through index_delete_check_htid
- Handles HOT chains by traversing from index-pointed tuple through the entire update chain  
- For bottom-up deletion, implements intelligent termination when space targets are met
- Returns InvalidTransactionId conflict horizon when no conflicts are needed
- Final deltids array may be shrunk to exclude non-deletable entries
- Critical for index AM performance during bulk deletion operations like VACUUM