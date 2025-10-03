# heap_set_tidrange

## Location
[src/backend/access/heap/heapam.c:1375-1447](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L1375-L1447)

## Overview
Configures a heap table scan to limit scanning to tuples within a specified TID (tuple identifier) range, optimizing scans that only need to examine specific portions of a table.

## Definition

```c
void
heap_set_tidrange(TableScanDesc sscan, ItemPointer mintid,
				  ItemPointer maxtid)
```
## Detailed Description
The  function restricts a heap table scan to only examine tuples within a specified range of tuple identifiers (TIDs). It calculates the actual block range that needs to be scanned based on the provided minimum and maximum TIDs, handles edge cases like empty relations and invalid ranges, and updates the scan descriptor with the computed limits. This is particularly useful for TID range scans where only specific portions of a table need to be examined, providing significant performance benefits by avoiding unnecessary block reads.

The function performs careful bounds checking against the relation's actual size, handles empty ranges gracefully, and optimizes the scan by calculating the minimum number of blocks that need to be examined to cover the requested TID range.

## Parameters / Member Variables
- : The table scan descriptor to configure (cast to HeapScanDesc internally)
- : Pointer to the minimum TID (inclusive) for the scan range
- : Pointer to the maximum TID (inclusive) for the scan range

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerSet](../I/ItemPointerSet.md)
  - [ItemPointerCompare](../I/ItemPointerCompare.md)
  - [ItemPointerCopy](../I/ItemPointerCopy.md)
  - [ItemPointerGetBlockNumberNoCheck](../I/ItemPointerGetBlockNumberNoCheck.md)
  - [heap_setscanlimits](heap_setscanlimits.md)
- Data structures used:
  - [HeapScanDesc](../H/HeapScanDesc.md)
  - [TableScanDesc](../T/TableScanDesc.md)
  - ItemPointer
  - [ItemPointerData](../I/ItemPointerData.md)
  - BlockNumber
- Constants:
  - MaxOffsetNumber
  - FirstOffsetNumber
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md)
  - HeapScanIsValid

## Notes and Other Information
- Returns early for relations with no pages () since there are no tuples to scan
- Creates boundary ItemPointers representing the lowest possible TID  and highest possible TID 
- Clamps the requested range to the actual bounds of the relation to prevent scanning non-existent blocks
- Detects empty ranges (where highestItem < lowestItem) and sets up an empty scan with 
- Could be optimized further by checking offset boundaries (e.g., advancing startBlk if lowestItem offset > MaxOffsetNumber) but such optimizations are currently considered not worth the complexity
- Sets both the block-level scan limits via  and the exact TID range in / for precise tuple filtering
- The TID range checking ensures that scans don't attempt to read beyond the actual size of the relation

## Simplified Source

```c
void heap_set_tidrange(TableScanDesc sscan, ItemPointer mintid, ItemPointer maxtid) {
    HeapScanDesc scan = (HeapScanDesc) sscan;
    BlockNumber startBlk, numBlks;
    ItemPointerData highestItem, lowestItem;

    // Handle empty relations
    if (scan->rs_nblocks == 0)
        return;

    // Set up boundary TIDs for the relation
    ItemPointerSet(&highestItem, scan->rs_nblocks - 1, MaxOffsetNumber);
    ItemPointerSet(&lowestItem, 0, FirstOffsetNumber);

    // Clamp maximum TID to relation bounds
    if (ItemPointerCompare(maxtid, &highestItem) < 0)
        ItemPointerCopy(maxtid, &highestItem);

    // Clamp minimum TID to relation bounds
    if (ItemPointerCompare(mintid, &lowestItem) > 0)
        ItemPointerCopy(mintid, &lowestItem);

    // Check for empty range
    if (ItemPointerCompare(&highestItem, &lowestItem) < 0) {
        heap_setscanlimits(sscan, 0, 0);  // Empty scan
        return;
    }

    // Calculate block range to scan
    startBlk = ItemPointerGetBlockNumberNoCheck(&lowestItem);
    numBlks = ItemPointerGetBlockNumberNoCheck(&highestItem) -
              ItemPointerGetBlockNumberNoCheck(&lowestItem) + 1;

    // Set scan limits and TID range
    heap_setscanlimits(sscan, startBlk, numBlks);
    ItemPointerCopy(&lowestItem, &sscan->rs_mintid);
    ItemPointerCopy(&highestItem, &sscan->rs_maxtid);
}
```