# _bt_load

## Location
src/backend/access/nbtree/nbtsort.c: 1135 - 1395

## Overview
The main function that reads sorted tuples from tuplesort and loads them into B-tree leaf pages, handling merging of multiple tuple sources, deduplication, and progress reporting.

## Definition

```c
static void
_bt_load(BTWriteState *wstate, BTSpool *btspool, BTSpool *btspool2)
```
## Detailed Description
This function is the core of B-tree index construction from sorted data. It reads tuples in correct sort order from one or two tuplesort sources and efficiently loads them into B-tree leaf pages. The function handles three distinct operational modes:

1. **Merge Mode**: When  is provided (typically for dead tuples), the function merges two sorted streams while maintaining proper sort order. It sets up SortSupport structures for efficient comparison and uses a sophisticated comparison algorithm that considers all key attributes plus heap TID for uniqueness.

2. **Deduplication Mode**: When deduplication is enabled ( is true, index is not unique, and BTGetDeduplicateItems returns true), the function groups identical key values into posting lists to reduce index size. It maintains a BTDedupState to track pending items and creates posting list tuples when multiple heap TIDs share the same key.

3. **Simple Mode**: When neither merging nor deduplication is needed, the function directly adds each tuple to the index without additional processing.

Key features and optimizations:
- **Progress Reporting**: Updates progress statistics to track tuples processed
- **Memory Management**: Properly allocates and frees temporary structures
- **Posting List Size Limits**: Limits posting list tuples to 1/10 of available space to maintain reasonable page utilization
- **Bulk Writing**: Uses bulk write operations for efficient I/O during index construction

The function coordinates with other components like  for page management and  for finalizing the index structure.

## Parameters / Member Variables
- : BTWriteState structure containing the overall state of index building, including the target index relation and bulk write state
- : Primary BTSpool containing the main sorted tuple stream from the tuplesort
- : Optional secondary BTSpool containing additional sorted tuples (typically dead tuples), or NULL if no merging is required

## Dependencies
- Functions called/Symbols referenced:
  - smgr_bulk_start_rel
  - BTGetDeduplicateItems
  - tuplesort_getindextuple
  - palloc0
  - CurrentMemoryContext
  - PrepareSortSupportFromIndexRel
  - index_getattr
  - ApplySortComparator
  - ItemPointerCompare
  - _bt_pagestate
  - _bt_buildadd
  - pgstat_progress_update_param
  - pfree
  - palloc
  - InvalidOffsetNumber
  - MAXALIGN_DOWN
  - BTMaxItemSize
  - CopyIndexTuple
  - _bt_dedup_start_pending
  - _bt_keep_natts_fast
  - _bt_dedup_save_htid
  - _bt_sort_dedup_finish_pending
  - _bt_uppershutdown
  - smgr_bulk_finish
- Called from (representative examples):
  - _bt_leafbuild

## Notes and Other Information
- This function is central to PostgreSQL's B-tree index creation process and handles the most performance-critical phase
- The merge logic ensures that when dead tuples need to be included, they are properly interleaved with live tuples in sort order
- Deduplication can significantly reduce index size for tables with many duplicate values, particularly beneficial for non-unique indexes
- The SortSupport infrastructure provides optimized comparison functions for different data types
- Heap TID comparison ensures deterministic ordering even when key values are identical
- Progress reporting enables monitoring of long-running index creation operations
- The bulk write mechanism provides significant performance improvements over individual page writes
- Posting list size limits prevent excessively large tuples that could impact page utilization and query performance
- The function properly handles edge cases like empty tuple streams and ensures clean memory management throughout