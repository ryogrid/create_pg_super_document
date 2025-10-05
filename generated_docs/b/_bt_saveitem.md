# _bt_saveitem

## Location
[src/backend/access/nbtree/nbtsearch.c:1945-1974](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsearch.c#L1945-L1974)

## Overview
Saves a non-pivot, non-posting index tuple into the current scan position's item array for B-tree scanning operations.

## Definition

```c
static void
_bt_saveitem(BTScanOpaque so, int itemIndex,
			 OffsetNumber offnum, IndexTuple itup)
```
## Detailed Description
This function is a helper routine used during B-tree page scanning to store index tuples in the scan state's current position structure. It specifically handles regular index tuples (not pivot tuples or posting tuples) by copying the heap TID, storing the page offset, and optionally copying the entire tuple data if tuple caching is enabled. The function ensures proper memory alignment when storing tuple data and maintains the scan state's tuple offset counter.

## Parameters / Member Variables
- `so`: B-tree scan opaque structure containing the current scan state
- `itemIndex`: Index position in the items array where this tuple should be stored
- `offnum`: Offset number of the tuple on the current page
- `itup`: The index tuple to be saved (must not be a pivot or posting tuple)
## Dependencies
- Functions called/Symbols referenced:
  - [BTreeTupleIsPivot](../B/BTreeTupleIsPivot.md) (assertion check)
  - [BTreeTupleIsPosting](../B/BTreeTupleIsPosting.md) (assertion check)
  - IndexTupleSize (for tuple size calculation)
- Called from (representative examples):
  - [_bt_readpage](_bt_readpage.md) (multiple calls during page scanning)

## Notes and Other Information
- This function includes assertions to ensure the tuple is neither a pivot tuple nor a posting tuple, as these have different handling requirements
- Tuple copying is conditional based on whether  is allocated, allowing for memory-efficient scanning when full tuple data isn't needed
- Uses MAXALIGN to ensure proper memory alignment of stored tuples
- Part of the B-tree scanning infrastructure in nbtsearch.c

## Simplified Source

```c
static void
_bt_saveitem(BTScanOpaque so, int itemIndex,
             OffsetNumber offnum, IndexTuple itup)
{
    BTScanPosItem *currItem = &so->currPos.items[itemIndex];

    // Store heap tuple ID and page offset
    currItem->heapTid = itup->t_tid;
    currItem->indexOffset = offnum;

    // Copy tuple data if caching is enabled
    if (so->currTuples) {
        Size tupleSize = IndexTupleSize(itup);
        currItem->tupleOffset = so->currPos.nextTupleOffset;

        // Copy tuple data with proper alignment
        memcpy(so->currTuples + so->currPos.nextTupleOffset, itup, tupleSize);
        so->currPos.nextTupleOffset += MAXALIGN(tupleSize);
    }
}
```