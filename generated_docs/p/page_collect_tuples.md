# page_collect_tuples

## Location
[src/backend/access/heap/heapam.c:488-537](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L488-L537)

## Overview
page_collect_tuples is an inline helper function that scans through all tuples on a heap page and collects visible tuple offsets, serving as the core tuple visibility filtering loop for heap page scanning operations.

## Definition

```c
static int
page_collect_tuples(HeapScanDesc scan, Snapshot snapshot,
					Page page, Buffer buffer,
					BlockNumber block, int lines,
					bool all_visible, bool check_serializable)
```
## Detailed Description
This function iterates through all item identifiers on a heap page and evaluates each tuple's visibility according to the provided snapshot. It's designed as an always-inline function to maximize performance during sequential scans. The function handles both optimized paths (when all tuples are known to be visible) and general paths (requiring full visibility checks). For each visible tuple, it records the offset number in the scan descriptor's rs_vistuples array, enabling efficient tuple retrieval in subsequent operations.

## Parameters / Member Variables
- `scan`: HeapScanDesc containing the scan state and result storage
- `snapshot`: Snapshot defining transaction visibility rules
- `page`: The heap page being scanned
- `buffer`: Buffer containing the page (needed for visibility checks)
- `block`: Block number of the page being scanned
- `lines`: Number of line pointers (item identifiers) on the page
- `all_visible`: Optimization flag indicating all tuples are known to be visible
- `check_serializable`: Whether to perform serializable conflict detection
## Dependencies
- Functions called/Symbols referenced:
  - [PageGetItemId](../P/PageGetItemId.md)
  - ItemIdIsNormal
  - [PageGetItem](../P/PageGetItem.md)
  - ItemIdGetLength
  - RelationGetRelid
  - [ItemPointerSet](../I/ItemPointerSet.md)
  - [HeapTupleSatisfiesVisibility](../H/HeapTupleSatisfiesVisibility.md)
  - [HeapCheckForSerializableConflictOut](../H/HeapCheckForSerializableConflictOut.md)
- Called from (representative examples):
  - [heap_prepare_pagescan](../h/heap_prepare_pagescan.md) (multiple call sites)

## Notes and Other Information
- Marked as pg_attribute_always_inline for performance optimization
- Returns the number of visible tuples found (ntup)
- Uses MaxHeapTuplesPerPage assertion to ensure array bounds safety
- Optimizes visibility checking when all_visible is true
- Handles serializable isolation level conflict detection when required
- Core building block for heap scanning operations in PostgreSQL

## Simplified Source

```c
static int
page_collect_tuples(HeapScanDesc scan, Snapshot snapshot,
                   Page page, Buffer buffer,
                   BlockNumber block, int lines,
                   bool all_visible, bool check_serializable)
{
    int ntup = 0;
    OffsetNumber lineoff;

    // Iterate through all line pointers on the page
    for (lineoff = FirstOffsetNumber; lineoff <= lines; lineoff++)
    {
        ItemId lpp = PageGetItemId(page, lineoff);
        HeapTupleData loctup;
        bool valid;

        // Skip non-normal items (dead, redirected, etc.)
        if (!ItemIdIsNormal(lpp))
            continue;

        // Set up tuple structure
        loctup.t_data = (HeapTupleHeader) PageGetItem(page, lpp);
        loctup.t_len = ItemIdGetLength(lpp);
        loctup.t_tableOid = RelationGetRelid(scan->rs_base.rs_rd);
        ItemPointerSet(&(loctup.t_self), block, lineoff);

        // Check tuple visibility
        if (all_visible)
            valid = true;  // Fast path: all tuples are visible
        else
            valid = HeapTupleSatisfiesVisibility(&loctup, snapshot, buffer);

        // Check for serializable conflicts if needed
        if (check_serializable)
            HeapCheckForSerializableConflictOut(valid, scan->rs_base.rs_rd,
                                              &loctup, buffer, snapshot);

        // Record visible tuple offset
        if (valid)
        {
            scan->rs_vistuples[ntup] = lineoff;
            ntup++;
        }
    }

    Assert(ntup <= MaxHeapTuplesPerPage);
    return ntup;
}
```