# entryGetItem

## Location
[src/backend/access/gin/ginget.c:810-991](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginget.c#L810-L991)

## Overview
Advances a GIN scan entry to the next heap item pointer greater than a specified position, handling three different data sources: bitmap results, posting lists, and posting trees.

## Definition

```c
static void
entryGetItem(GinState *ginstate, GinScanEntry entry,
			 ItemPointerData advancePast)
```
## Detailed Description
The entryGetItem function is the core mechanism for iterating through item pointers within a single GIN scan entry. It implements three distinct algorithms depending on the data source: bitmap results from potentially large result sets, posting lists from entry tuples or final posting tree pages, and posting trees requiring incremental loading.

For bitmap results, the function uses TIDBitmap iteration to efficiently handle potentially large result sets, supporting both exact item pointers and lossy page pointers. It carefully manages the transition between blocks and handles lossy pages specially to maintain the constraint that lossy and exact pointers cannot be mixed for the same page.

For posting lists (when buffer is invalid), it performs simple array traversal through pre-loaded items. For posting trees (when buffer is valid), it implements incremental loading by calling entryLoadMoreItems when the current batch is exhausted, enabling efficient processing of very large posting lists without memory bloat.

The function also implements result reduction logic when GinFuzzySearchLimit is active, using the dropItem function to probabilistically skip items and reduce result set size for performance.

## Parameters / Member Variables
- : Pointer to GIN state containing index metadata and configuration
- : GIN scan entry to advance, containing current position and data source information
- : Item pointer indicating the minimum position for the next item to return

## Dependencies
- Functions called/Symbols referenced:
  - [ginCompareItemPointers](../g/ginCompareItemPointers.md)
  - [entryLoadMoreItems](entryLoadMoreItems.md)
  - [tbm_iterate](../t/tbm_iterate.md)
  - [tbm_end_iterate](../t/tbm_end_iterate.md)
  - dropItem
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md)
  - ItemPointerSetLossyPage
  - [ItemPointerSet](../I/ItemPointerSet.md)
  - [ItemPointerIsValid](../I/ItemPointerIsValid.md)
  - ItemPointerIsLossyPage
  - GinItemPointerGetBlockNumber
  - GinItemPointerGetOffsetNumber
- Data types used:
  - [GinState](../G/GinState.md)
  - [GinScanEntry](../G/GinScanEntry.md)
  - [ItemPointerData](../I/ItemPointerData.md)
  - BlockNumber
  - OffsetNumber
- Called from:
  - [keyGetItem](../k/keyGetItem.md) (multiple times)

## Notes and Other Information
- Critical constraint: cannot return both lossy page pointers and exact item pointers for the same heap page
- Supports three data sources with optimized algorithms for each: bitmaps, lists, and trees
- Implements result reduction for fuzzy search limits to control performance vs. completeness trade-offs
- The function maintains proper iteration state across calls, enabling resumable scanning
- For posting trees, coordinates with entryLoadMoreItems to implement lazy loading of large posting lists
- Returns items in ascending ItemPointer order as required by higher-level key combination logic
- Handles edge cases like empty result sets and transitions between different result types gracefully