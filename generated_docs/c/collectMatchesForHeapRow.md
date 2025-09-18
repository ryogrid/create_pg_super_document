# collectMatchesForHeapRow

## Location
[src/backend/access/gin/ginget.c:1609-1823](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginget.c#L1609-L1823)

## Overview
Examines all pending list entries for a current heap row and populates the entryRes array for each scan key, determining if the row satisfies search criteria.

## Definition
```c
static bool collectMatchesForHeapRow(IndexScanDesc scan, pendingPosition *pos)
```

## Detailed Description
This function processes all pending entries belonging to a single heap row across potentially multiple pages, building a complete picture of which scan entry conditions are satisfied. It uses binary search optimization to efficiently locate matching entries within the ordered pending list structure, taking advantage of the (attnum, Datum) ordering.

The function handles both exact matches and partial matches, with special logic for EMPTY_QUERY entries that have different matching semantics. For heap rows spanning multiple pages, it coordinates with scanGetCandidate to process all relevant pages while maintaining proper position tracking.

Performance is optimized through datum caching arrays that prevent redundant key extraction operations when the same tuple is examined by multiple scan entries. The function returns true only when all non-excludeOnly scan keys have at least one matching entry.

## Parameters / Member Variables
- `scan`: Index scan descriptor containing scan keys and state information
- `pos`: Pending position structure defining the heap row's tuple range and managing page transitions

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md)
  - [gintuple_get_attrnum](../g/gintuple_get_attrnum.md)
  - [gintuple_get_key](../g/gintuple_get_key.md)
  - [ginCompareEntries](../g/ginCompareEntries.md)
  - [matchPartialInPendingList](../m/matchPartialInPendingList.md)
  - GinPageHasFullRow
  - [scanGetCandidate](../s/scanGetCandidate.md)
  - [ItemPointerEquals](../I/ItemPointerEquals.md)
- Called from (representative examples):
  - [scanPendingInsert](../s/scanPendingInsert.md)

## Notes and Other Information
Central component of pending list processing that determines whether heap rows satisfy query conditions. The binary search optimization is crucial for performance on large pending lists. The function must correctly handle the transition between pages when a single heap row's entries span multiple pending list pages, which can occur during high-volume insert scenarios.