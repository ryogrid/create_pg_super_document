# _bt_setuppostingitems

## Location
[src/backend/access/nbtree/nbtsearch.c:1975-2012](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsearch.c#L1975-L2012)

## Overview
Sets up the scan state to handle TIDs from a posting list tuple, saving the first TID and preparing for subsequent TID processing.

## Definition
```c
static int _bt_setuppostingitems(BTScanOpaque so, int itemIndex, OffsetNumber offnum, ItemPointer heapTid, IndexTuple itup)
```

## Detailed Description
This function initializes the processing of a posting list tuple during B-tree scanning. Posting list tuples contain multiple heap TIDs for the same key value, compressed into a single index tuple. The function saves the first TID into the scan position's item array and, if tuple caching is enabled, creates a truncated base tuple (without the posting list) in the tuple storage area. This base tuple can be used later for additional TID processing via _bt_savepostingitem().

## Parameters / Member Variables
- `so`: B-tree scan opaque structure containing the current scan state
- `itemIndex`: Index position in the items array where the first TID should be stored
- `offnum`: Offset number of the posting list tuple on the current page
- `heapTid`: Pointer to the first heap TID from the posting list to be saved
- `itup`: The posting list index tuple to be processed

## Dependencies
- Functions called/Symbols referenced:
  - [BTreeTupleIsPosting](../B/BTreeTupleIsPosting.md) (assertion check)
  - [BTreeTupleGetPostingOffset](../B/BTreeTupleGetPostingOffset.md) (to determine base tuple size)
  - INDEX_SIZE_MASK (for tuple header size manipulation)
- Called from (representative examples):
  - [_bt_readpage](_bt_readpage.md) (during posting list tuple processing)

## Notes and Other Information
- Returns the offset into tuple storage where the base tuple is stored, or 0 if tuple caching is disabled
- The function creates a defensive copy by truncating the posting list portion and adjusting the tuple header size
- Must be followed by calls to _bt_savepostingitem() for processing additional TIDs from the same posting list
- Only handles posting list tuples (verified by assertion)
- Part of PostgreSQL's posting list optimization for reducing index size when many tuples have identical key values

## Simplified Source

```c
static int
_bt_setuppostingitems(BTScanOpaque so, int itemIndex, OffsetNumber offnum,
                      ItemPointer heapTid, IndexTuple itup)
{
    BTScanPosItem *currItem = &so->currPos.items[itemIndex];

    // Store first TID from posting list and page offset
    currItem->heapTid = *heapTid;
    currItem->indexOffset = offnum;

    // Create base tuple (without posting list) if caching enabled
    if (so->currTuples) {
        // Calculate base tuple size (excludes posting list)
        Size baseSize = BTreeTupleGetPostingOffset(itup);
        baseSize = MAXALIGN(baseSize);

        currItem->tupleOffset = so->currPos.nextTupleOffset;
        IndexTuple base = (IndexTuple)(so->currTuples + so->currPos.nextTupleOffset);

        // Copy base tuple data and adjust header size
        memcpy(base, itup, baseSize);
        base->t_info &= ~INDEX_SIZE_MASK;
        base->t_info |= baseSize;

        so->currPos.nextTupleOffset += baseSize;
        return currItem->tupleOffset;
    }

    return 0;
}
```