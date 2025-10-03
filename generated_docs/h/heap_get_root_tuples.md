# heap_get_root_tuples

## Location
[src/backend/access/heap/pruneheap.c:1785-1895](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/pruneheap.c#L1785-L1895)

## Overview
Maps each tuple on a heap page to its root line pointer in HOT (Heap Only Tuples) chains, providing essential information for index operations and tuple visibility.

## Definition

```c
void
heap_get_root_tuples(Page page, OffsetNumber *root_offsets)
```
## Detailed Description
This function analyzes a heap page and constructs a mapping that identifies the root tuple for each item in HOT chains. For each line pointer offset k on the page, if that item is part of a HOT chain with root at offset j, then root_offsets[k-1] is set to j. Non-HOT tuples point to themselves as roots.

The function performs a comprehensive scan of the page, identifying HOT chain structures by following t_ctid pointers and validating transaction relationships between chain members. It handles both normal tuples and redirect items, ensuring that the entire chain structure is properly mapped. The algorithm runs in O(N) time complexity despite containing nested loops, as each tuple is visited at most twice.

This mapping is crucial for index operations, as indexes point to root tuples, and this function helps determine which tuples are actually visible through index scans.

## Parameters / Member Variables
- `page`: The heap page to analyze for HOT chain structure
- `*root_offsets`: Output array with MaxHeapTuplesPerPage entries where root_offsets[k-1] contains the root offset for item at offset k, or InvalidOffsetNumber for unused items
## Dependencies
- Functions called/Symbols referenced:
  - MemSet
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - ItemIdIsUsed
  - ItemIdIsDead
  - ItemIdIsNormal
  - ItemIdIsRedirected
  - ItemIdGetRedirect
  - [PageGetItem](../P/PageGetItem.md)
  - HeapTupleHeaderIsHeapOnly
  - HeapTupleHeaderIsHotUpdated
  - HeapTupleHeaderIndicatesMovedPartitions
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
  - HeapTupleHeaderGetUpdateXid
  - HeapTupleHeaderGetXmin
  - TransactionIdEquals
  - FirstOffsetNumber
  - OffsetNumberNext
- Called from (representative examples):
  - [heapam_index_build_range_scan](heapam_index_build_range_scan.md)
  - [heapam_index_validate_scan](heapam_index_validate_scan.md)

## Notes and Other Information
- Requires at least share lock on the buffer to prevent concurrent prune operations
- The mapping is valid only while the caller holds a pin on the buffer
- Unused entries in root_offsets are filled with InvalidOffsetNumber
- Handles broken HOT chains gracefully by stopping chain traversal
- Critical for index building and validation operations
- Used primarily in heap access method implementations for index operations

## Simplified Source

```c
void
heap_get_root_tuples(Page page, OffsetNumber *root_offsets)
{
    OffsetNumber offnum, maxoff;

    // Initialize all entries to invalid
    MemSet(root_offsets, InvalidOffsetNumber, MaxHeapTuplesPerPage * sizeof(OffsetNumber));

    maxoff = PageGetMaxOffsetNumber(page);

    // Scan all items on the page
    for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
        ItemId lp = PageGetItemId(page, offnum);
        HeapTupleHeader htup;
        OffsetNumber nextoffnum;
        TransactionId priorXmax;

        // Skip unused and dead items
        if (!ItemIdIsUsed(lp) || ItemIdIsDead(lp))
            continue;

        if (ItemIdIsNormal(lp)) {
            htup = (HeapTupleHeader) PageGetItem(page, lp);

            // Skip heap-only tuples - they'll be processed via their root
            if (HeapTupleHeaderIsHeapOnly(htup))
                continue;

            // This is a root tuple (normal or start of HOT chain)
            root_offsets[offnum - 1] = offnum;

            // If not HOT-updated, we're done with this tuple
            if (!HeapTupleHeaderIsHotUpdated(htup))
                continue;

            // Start following the HOT chain
            nextoffnum = ItemPointerGetOffsetNumber(&htup->t_ctid);
            priorXmax = HeapTupleHeaderGetUpdateXid(htup);
        }
        else {
            // Redirect item - start of HOT chain
            Assert(ItemIdIsRedirected(lp));
            nextoffnum = ItemIdGetRedirect(lp);
            priorXmax = InvalidTransactionId;
        }

        // Follow the HOT chain and map all members to this root
        for (;;) {
            if (nextoffnum < FirstOffsetNumber || nextoffnum > maxoff)
                break;

            lp = PageGetItemId(page, nextoffnum);
            if (!ItemIdIsNormal(lp))
                break;

            htup = (HeapTupleHeader) PageGetItem(page, lp);

            // Validate chain consistency
            if (TransactionIdIsValid(priorXmax) &&
                !TransactionIdEquals(priorXmax, HeapTupleHeaderGetXmin(htup)))
                break;

            // Map this tuple to the root
            root_offsets[nextoffnum - 1] = offnum;

            // Continue to next in chain if HOT-updated
            if (!HeapTupleHeaderIsHotUpdated(htup))
                break;

            nextoffnum = ItemPointerGetOffsetNumber(&htup->t_ctid);
            priorXmax = HeapTupleHeaderGetUpdateXid(htup);
        }
    }
}
```