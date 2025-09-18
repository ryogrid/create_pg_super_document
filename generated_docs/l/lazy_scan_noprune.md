# lazy_scan_noprune

## Location
[src/backend/access/heap/vacuumlazy.c:1654-1864](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/vacuumlazy.c#L1654-L1864)

## Overview
A lightweight heap page scanning function that processes pages without performing pruning or freezing, collecting dead items and statistics while requiring only a shared buffer lock.

## Definition
```c
static bool lazy_scan_noprune(LVRelState *vacrel,
                             Buffer buf,
                             BlockNumber blkno,
                             Page page,
                             bool *has_lpdead_items)
```

## Detailed Description
lazy_scan_noprune is an optimized variant of lazy_scan_prune that processes heap pages when a full cleanup lock cannot be obtained or is not needed. It scans all tuples on the page to collect statistics and identify LP_DEAD items left by previous pruning operations, but does not perform any actual pruning or freezing. The function serves as a fallback when VACUUM cannot get exclusive access to a page, allowing it to still gather useful information and handle LP_DEAD items for index cleanup. For aggressive VACUUMs, it may return false to indicate that full processing with lazy_scan_prune is required when tuple freezing is necessary to advance the relation's freeze parameters.

## Parameters / Member Variables
- `vacrel`: LVRelState containing VACUUM operation state and configuration
- `buf`: Buffer containing the heap page to process (shared lock sufficient)
- `blkno`: Block number of the page being processed
- `page`: Pointer to the actual page data
- `has_lpdead_items`: Output parameter indicating if LP_DEAD items were found on the page

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - ItemIdIsUsed
  - ItemIdIsRedirected
  - ItemIdIsDead
  - [PageGetItem](../P/PageGetItem.md)
  - [heap_tuple_should_freeze](../h/heap_tuple_should_freeze.md)
  - [ItemPointerSet](../I/ItemPointerSet.md)
  - ItemIdGetLength
  - [HeapTupleSatisfiesVacuum](../H/HeapTupleSatisfiesVacuum.md)
  - [dead_items_add](../d/dead_items_add.md)
- Called from:
  - [lazy_scan_heap](lazy_scan_heap.md)

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - ItemIdIsUsed
  - ItemIdIsRedirected
  - ItemIdIsDead
  - [PageGetItem](../P/PageGetItem.md)
  - [heap_tuple_should_freeze](../h/heap_tuple_should_freeze.md)
  - [ItemPointerSet](../I/ItemPointerSet.md)
  - ItemIdGetLength
  - [HeapTupleSatisfiesVacuum](../H/HeapTupleSatisfiesVacuum.md)
  - [dead_items_add](../d/dead_items_add.md)
- Called from:
  - [lazy_scan_heap](lazy_scan_heap.md)

## Notes and Other Information
- Returns true if processing completed successfully, false if aggressive VACUUM needs full pruning
- Requires only shared buffer lock unlike lazy_scan_prune which needs cleanup lock
- For aggressive VACUUMs, returns false when tuples need freezing to advance freeze horizons
- Handles special case of tables without indexes using single-pass strategy
- Counts various tuple states (live, recently dead, missed dead) for VACUUM statistics
- Does not modify the page content, only collects information and LP_DEAD items
- Updates vacrel statistics including live_tuples, recently_dead_tuples, and missed_dead_tuples