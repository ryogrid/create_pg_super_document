# _bt_getstackbuf

## Location
[src/backend/access/nbtree/nbtinsert.c:2319-2443](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtinsert.c#L2319-L2443)

## Overview
_bt_getstackbuf re-finds and locks a parent page containing the downlink to a specified child page, updating the stack information to reflect any changes due to concurrent operations.

## Definition

```c
Buffer
_bt_getstackbuf(Relation rel, Relation heaprel, BTStack stack, BlockNumber child)
```
## Detailed Description
This function addresses a fundamental challenge in concurrent B-tree operations: the location of parent-child relationships may change between the time a descent path is recorded and when it needs to be used later. The function performs several key operations:

1. **Initial Position**: Starts searching from the position recorded in the provided stack (bts_blkno and bts_offset), which represents where the downlink was expected to be found.

2. **Incomplete Split Handling**: If the target page has an incomplete split (marked by BTP_INCOMPLETE_SPLIT flag), it calls _bt_finish_split() to complete the split before proceeding.

3. **Linear Search Strategy**: Performs a two-phase linear search for the pivot tuple containing the downlink to the child page:
   - First phase: Searches rightward from the starting position (handling rightward movement due to insertions)
   - Second phase: Searches leftward from the starting position (handling leftward movement, though limited)

4. **Rightward Page Movement**: If the pivot tuple is not found on the current page, moves to the next page to the right and continues searching, since concurrent operations can only move pivot tuples rightward.

5. **Stack Update**: Updates the stack's bts_blkno and bts_offset fields to reflect the actual current location of the pivot tuple.

The function ensures that callers can reliably find parent-child relationships even in the presence of concurrent page splits and other structural changes.

## Parameters / Member Variables
- `rel`: The B-tree index relation being searched
- `heaprel`: The heap relation referenced by the index (required for potential incomplete split completion)
- `stack`: BTStack containing the expected location of the pivot tuple (updated with actual location)
- `child`: Block number of the child page whose parent downlink needs to be found
## Dependencies
- Functions called/Symbols referenced:
  - [_bt_getbuf](_bt_getbuf.md) (to acquire write locks on parent pages)
  - [_bt_finish_split](_bt_finish_split.md) (to complete incomplete splits when encountered)
  - [BTreeTupleGetDownLink](../B/BTreeTupleGetDownLink.md) (to extract downlink block numbers from pivot tuples)
  - [PageGetItemId](../P/PageGetItemId.md), PageGetItem (for accessing page items)
  - P_INCOMPLETE_SPLIT, P_IGNORE, P_RIGHTMOST (page status flags)
  - Various offset number manipulation functions
- Called from (representative examples):
  - [_bt_insert_parent](_bt_insert_parent.md) (to re-find parent page during split completion)
  - [_bt_lock_subtree_parent](_bt_lock_subtree_parent.md) (for page deletion operations)

## Notes and Other Information
- Returns a write-locked buffer containing the parent page, or InvalidBuffer if not found
- The search algorithm is optimized for the common case where the pivot tuple hasn't moved far
- Handles the "moving right" property of Lehman & Yao B-trees where concurrent operations can only push pivot tuples rightward
- The function is designed to be resilient to concurrent page splits and other structural modifications
- Automatically handles incomplete splits by completing them before continuing the search
- Updates stack information to provide accurate positioning for subsequent operations like pivot tuple insertion
- The two-phase search (right first, then left) is tuned to the probability distribution of where pivot tuples are likely to be found
- Part of the larger mechanism that enables lock-coupling in concurrent B-tree operations
- Critical for maintaining consistency during complex operations like page splits that span multiple tree levels

## Simplified Source

```c
Buffer
_bt_getstackbuf(Relation rel, Relation heaprel, BTStack stack, BlockNumber child)
{
    BlockNumber blkno;
    OffsetNumber start;

    blkno = stack->bts_blkno;
    start = stack->bts_offset;

    // Loop through pages until we find the downlink
    for (;;)
    {
        Buffer buf;
        Page page;
        BTPageOpaque opaque;

        // Lock the current parent page
        buf = _bt_getbuf(rel, blkno, BT_WRITE);
        page = BufferGetPage(buf);
        opaque = BTPageGetOpaque(page);

        Assert(heaprel != NULL);

        // Handle incomplete splits first
        if (P_INCOMPLETE_SPLIT(opaque))
        {
            _bt_finish_split(rel, heaprel, buf, stack->bts_parent);
            continue;
        }

        // Search the page for the downlink to child
        if (!P_IGNORE(opaque))
        {
            OffsetNumber offnum, minoff, maxoff;
            ItemId itemid;
            IndexTuple item;

            minoff = P_FIRSTDATAKEY(opaque);
            maxoff = PageGetMaxOffsetNumber(page);

            // Adjust start position if needed
            if (start < minoff)
                start = minoff;
            if (start > maxoff)
                start = OffsetNumberNext(maxoff);

            // Search right from start position
            for (offnum = start; offnum <= maxoff; offnum = OffsetNumberNext(offnum))
            {
                itemid = PageGetItemId(page, offnum);
                item = (IndexTuple) PageGetItem(page, itemid);

                if (BTreeTupleGetDownLink(item) == child)
                {
                    // Found it! Update stack and return
                    stack->bts_blkno = blkno;
                    stack->bts_offset = offnum;
                    return buf;
                }
            }

            // Search left from start position
            for (offnum = OffsetNumberPrev(start); offnum >= minoff; offnum = OffsetNumberPrev(offnum))
            {
                itemid = PageGetItemId(page, offnum);
                item = (IndexTuple) PageGetItem(page, itemid);

                if (BTreeTupleGetDownLink(item) == child)
                {
                    // Found it! Update stack and return
                    stack->bts_blkno = blkno;
                    stack->bts_offset = offnum;
                    return buf;
                }
            }
        }

        // Not found on this page, move right
        if (P_RIGHTMOST(opaque))
        {
            _bt_relbuf(rel, buf);
            return InvalidBuffer;  // Not found
        }

        blkno = opaque->btpo_next;
        start = InvalidOffsetNumber;
        _bt_relbuf(rel, buf);
    }
}
```