# heapam_scan_bitmap_next_block

## Location
[src/backend/access/heap/heapam_handler.c:2122-2254](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L2122-L2254)

## Overview
Processes the next block during a bitmap heap scan, examining tuples on the specified block and collecting visible tuples for subsequent retrieval.

## Definition

```c
static bool
heapam_scan_bitmap_next_block(TableScanDesc scan,
							  TBMIterateResult *tbmres)
```
## Detailed Description
This function is a core component of bitmap heap scans, responsible for processing individual blocks identified by a bitmap index scan. It determines which tuples on a given block are visible to the current transaction and stores their offsets for later tuple retrieval. The function handles both lossy and non-lossy bitmap results, employs optimizations for all-visible pages, and manages HOT (Heap-Only Tuples) chains appropriately. It also performs necessary locking, pruning, and visibility checks while maintaining transaction isolation guarantees.

## Parameters / Member Variables
- : The table scan descriptor containing scan state and parameters
- : Bitmap iterator result containing block number, tuple offsets, and metadata about the block

## Dependencies
- Functions called/Symbols referenced:
  - VM_ALL_VISIBLE (visibility map check)
  - IsolationIsSerializable (isolation level check)
  - ReleaseAndReadBuffer (buffer management)
  - [heap_page_prune_opt](heap_page_prune_opt.md) (page maintenance)
  - [heap_hot_search_buffer](heap_hot_search_buffer.md) (HOT chain traversal)
  - [HeapTupleSatisfiesVisibility](../H/HeapTupleSatisfiesVisibility.md) (tuple visibility)
  - [PredicateLockTID](../P/PredicateLockTID.md) (predicate locking)
  - [HeapCheckForSerializableConflictOut](../H/HeapCheckForSerializableConflictOut.md) (serializable isolation)
  - Various page and item manipulation functions
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md) (as part of table access method interface)

## Notes and Other Information
- Implements smart optimizations: skips fetching pages when tuples aren't needed and all tuples are visible
- Handles both lossy and non-lossy bitmap cases with different strategies
- For non-lossy bitmaps: follows HOT chains from specific offsets
- For lossy bitmaps: examines every line pointer on the page
- Maintains proper buffer locking discipline for concurrent safety
- Respects transaction isolation levels, especially SERIALIZABLE
- Updates scan state (rs_ntuples, rs_vistuples) for subsequent tuple fetching
- Returns true if any visible tuples were found on the block