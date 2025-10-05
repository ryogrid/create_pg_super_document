# _bt_simpledel_pass

## Location
[src/backend/access/nbtree/nbtinsert.c:2812-2937](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtinsert.c#L2812-L2937)

## Overview
Performs simple index tuple deletion by removing LP_DEAD-marked tuples and additional safe-to-delete tuples that share table blocks with dead tuples, optimizing deletion efficiency through locality-based batching.

## Definition
```c
static void _bt_simpledel_pass(Relation rel, Buffer buffer, Relation heapRel, OffsetNumber *deletable, int ndeletable, IndexTuple newitem, OffsetNumber minoff, OffsetNumber maxoff)
```

## Detailed Description
The `_bt_simpledel_pass` function implements the simple deletion strategy for B-tree leaf pages. It not only removes tuples explicitly marked as LP_DEAD but also opportunistically identifies and removes additional tuples that can be safely deleted without significant overhead.

The key optimization is locality-based deletion: the function identifies all table blocks referenced by LP_DEAD tuples, then scans all tuples on the page to find additional tuples pointing to the same table blocks. This approach leverages the principle that if we're already checking certain table blocks for deletability, we might as well check all tuples pointing to those blocks with minimal additional cost.

The function handles both regular index tuples and posting list tuples (used in deduplication), processing all TIDs within posting lists to maximize the benefit of each table block access. The tableam (table access method) layer performs the actual deletability checks and physical deletion.

## Parameters / Member Variables
- `rel`: The index relation being modified
- `buffer`: Buffer containing the leaf page to process
- `heapRel`: The corresponding heap relation for deletability checks
- `deletable`: Array of offset numbers for LP_DEAD-marked tuples
- `ndeletable`: Number of entries in the deletable array
- `newitem`: New item being inserted (used for block analysis)
- `minoff`: First offset number to consider on the page
- `maxoff`: Last offset number to consider on the page

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_deadblocks](_bt_deadblocks.md): Gets array of table blocks pointed to by dead tuples
  - [_bt_delitems_delete_check](_bt_delitems_delete_check.md): Performs actual deletion after tableam validation
  - [BTreeTupleIsPosting](../B/BTreeTupleIsPosting.md): Checks if tuple is a posting list tuple
  - [BTreeTupleGetNPosting](../B/BTreeTupleGetNPosting.md): Gets number of TIDs in posting list
  - [BTreeTupleGetPostingN](../B/BTreeTupleGetPostingN.md): Gets specific TID from posting list
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md): Extracts block number from TID
  - `[_bt_blk_cmp](_bt_blk_cmp.md)`: Comparison function for binary search of blocks
- Called from (representative examples):
  - [_bt_delete_or_dedup_one_page](_bt_delete_or_dedup_one_page.md): As the first deletion strategy

## Notes and Other Information
- Leverages locality effects where multiple index tuples often point to the same table blocks
- Uses binary search to efficiently match TIDs against dead block arrays
- Handles posting list tuples by processing each TID individually
- May delete significantly more tuples than just those marked LP_DEAD
- The number of extra deletable tuples can greatly exceed the number of originally dead tuples
- Builds TM_IndexDeleteOp structure to interface with the tableam layer
- Allocates temporary arrays sized for maximum possible TIDs per page
- Ensures at least the originally dead tuples are included in deletion candidates

## Simplified Source

```c
static void
_bt_simpledel_pass(Relation rel, Buffer buffer, Relation heapRel,
                  OffsetNumber *deletable, int ndeletable, IndexTuple newitem,
                  OffsetNumber minoff, OffsetNumber maxoff)
{
    Page page = BufferGetPage(buffer);
    BlockNumber *deadblocks;
    int ndeadblocks;
    TM_IndexDeleteOp delstate;

    // Get array of table blocks pointed to by LP_DEAD tuples (plus newitem block)
    deadblocks = _bt_deadblocks(page, deletable, ndeletable, newitem, &ndeadblocks);

    // Initialize deletion operation state
    delstate.irel = rel;
    delstate.iblknum = BufferGetBlockNumber(buffer);
    delstate.bottomup = false;
    delstate.bottomupfreespace = 0;
    delstate.ndeltids = 0;
    delstate.deltids = palloc(MaxTIDsPerBTreePage * sizeof(TM_IndexDelete));
    delstate.status = palloc(MaxTIDsPerBTreePage * sizeof(TM_IndexStatus));

    // Scan all tuples on page to find candidates for deletion
    for (OffsetNumber offnum = minoff; offnum <= maxoff; offnum++) {
        ItemId itemid = PageGetItemId(page, offnum);
        IndexTuple itup = (IndexTuple) PageGetItem(page, itemid);

        if (!BTreeTupleIsPosting(itup)) {
            // Regular tuple - check if its block is in our dead blocks list
            BlockNumber tidblock = ItemPointerGetBlockNumber(&itup->t_tid);
            void *match = bsearch(&tidblock, deadblocks, ndeadblocks,
                                sizeof(BlockNumber), _bt_blk_cmp);

            if (match) {
                // Add TID to deletion candidates
                TM_IndexDelete *odeltid = &delstate.deltids[delstate.ndeltids];
                TM_IndexStatus *ostatus = &delstate.status[delstate.ndeltids];

                odeltid->tid = itup->t_tid;
                odeltid->id = delstate.ndeltids;
                ostatus->idxoffnum = offnum;
                ostatus->knowndeletable = ItemIdIsDead(itemid);
                delstate.ndeltids++;
            }
        } else {
            // Posting list tuple - check each TID individually
            int nitem = BTreeTupleGetNPosting(itup);
            for (int p = 0; p < nitem; p++) {
                ItemPointer tid = BTreeTupleGetPostingN(itup, p);
                BlockNumber tidblock = ItemPointerGetBlockNumber(tid);
                void *match = bsearch(&tidblock, deadblocks, ndeadblocks,
                                    sizeof(BlockNumber), _bt_blk_cmp);

                if (match) {
                    // Add TID to deletion candidates
                    TM_IndexDelete *odeltid = &delstate.deltids[delstate.ndeltids];
                    TM_IndexStatus *ostatus = &delstate.status[delstate.ndeltids];

                    odeltid->tid = *tid;
                    odeltid->id = delstate.ndeltids;
                    ostatus->idxoffnum = offnum;
                    ostatus->knowndeletable = ItemIdIsDead(itemid);
                    delstate.ndeltids++;
                }
            }
        }
    }

    pfree(deadblocks);

    // Perform actual deletion through tableam layer
    _bt_delitems_delete_check(rel, buffer, heapRel, &delstate);

    pfree(delstate.deltids);
    pfree(delstate.status);
}
```