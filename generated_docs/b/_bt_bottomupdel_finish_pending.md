# _bt_bottomupdel_finish_pending

## Location
[src/backend/access/nbtree/nbtdedup.c:648-781](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtdedup.c#L648-L781)

## Overview
Finalizes an interval during bottom-up index deletion by moving TIDs from deduplication state to deletion state and determining which entries are duplicates for the tableam delete infrastructure.

## Definition
```c
static void _bt_bottomupdel_finish_pending(Page page, BTDedupState state, TM_IndexDeleteOp *delstate)
```

## Detailed Description
This function is called during a bottom-up deletion pass when the number of TIDs in a deduplication interval is known and the interval needs to be finalized. This happens when the caller encounters a non-duplicate tuple or runs out of tuples to process from the leaf page.

The function's primary responsibility is to determine and record which entries are duplicates, providing important information to the tableam delete infrastructure. It handles two main cases:

1. **Plain index tuples**: These are marked as "promising" if they are part of a duplicate interval, following a simple rule per the tableam contract.

2. **Posting list tuples**: These require more complex handling since they can only be formed by deduplication passes or during index builds. The function uses conservative heuristics to mark at most one TID per posting list as promising, based on which table block predominates in the posting list.

The function uses heuristics that work well in practice because it only needs to give the tableam a general idea about where to look for garbage, which tends to concentrate in relatively few table blocks.

## Parameters / Member Variables
- `page`: The B-tree leaf page being processed
- `state`: The deduplication state containing accumulated TIDs and interval information
- `delstate`: The index deletion operation state where finalized deletion candidates are stored

## Dependencies
- Functions called/Symbols referenced:
  - [PageGetItemId](../P/PageGetItemId.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [BTreeTupleIsPosting](../B/BTreeTupleIsPosting.md)
  - ItemIdGetLength
  - [BTreeTupleGetNPosting](../B/BTreeTupleGetNPosting.md)
  - [_bt_posting_valid](_bt_posting_valid.md)
  - [BTreeTupleGetHeapTID](../B/BTreeTupleGetHeapTID.md)
  - [BTreeTupleGetPostingN](../B/BTreeTupleGetPostingN.md)
  - [BTreeTupleGetMaxHeapTID](../B/BTreeTupleGetMaxHeapTID.md)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
- Called from:
  - [_bt_bottomupdel_pass](_bt_bottomupdel_pass.md)

## Notes and Other Information
- This is a static function within the nbtdedup.c module, part of PostgreSQL's B-tree deduplication system
- The function implements sophisticated heuristics for determining which entries should be marked as "promising" for deletion
- For posting list tuples, it conservatively assumes at most one affected logical row per tuple
- The promising flag helps the tableam prioritize which table blocks to examine during deletion operations
- Located at src/backend/access/nbtree/nbtdedup.c:648-781

## Simplified Source

```c
static void _bt_bottomupdel_finish_pending(Page page, BTDedupState state,
                                          TM_IndexDeleteOp *delstate) {
    bool dupinterval = (state->nitems > 1);
    Assert(state->nitems > 0 && state->nitems <= state->nhtids);

    // Process each item in the interval
    for (int i = 0; i < state->nitems; i++) {
        OffsetNumber offnum = state->baseoff + i;
        ItemId itemid = PageGetItemId(page, offnum);
        IndexTuple itup = (IndexTuple) PageGetItem(page, itemid);
        TM_IndexDelete *ideltid = &delstate->deltids[delstate->ndeltids];
        TM_IndexStatus *istatus = &delstate->status[delstate->ndeltids];

        if (!BTreeTupleIsPosting(itup)) {
            // Simple case: plain tuple - mark promising if in duplicate interval
            ideltid->tid = itup->t_tid;
            ideltid->id = delstate->ndeltids;
            istatus->idxoffnum = offnum;
            istatus->knowndeletable = false;
            istatus->promising = dupinterval; // Simple rule
            istatus->freespace = ItemIdGetLength(itemid) + sizeof(ItemIdData);
            delstate->ndeltids++;
        } else {
            // Complex case: posting list tuple
            int nitem = BTreeTupleGetNPosting(itup);
            bool firstpromising = false, lastpromising = false;

            if (dupinterval) {
                // Determine which TID to mark promising based on block distribution
                ItemPointer mintid = BTreeTupleGetHeapTID(itup);
                ItemPointer midtid = BTreeTupleGetPostingN(itup, nitem / 2);
                ItemPointer maxtid = BTreeTupleGetMaxHeapTID(itup);

                BlockNumber minblock = ItemPointerGetBlockNumber(mintid);
                BlockNumber midblock = ItemPointerGetBlockNumber(midtid);
                BlockNumber maxblock = ItemPointerGetBlockNumber(maxtid);

                // Mark based on predominant table block
                firstpromising = (minblock == midblock);
                lastpromising = (!firstpromising && midblock == maxblock);
            }

            // Add each TID from posting list to deletion state
            for (int p = 0; p < nitem; p++) {
                ItemPointer htid = BTreeTupleGetPostingN(itup, p);
                ideltid->tid = *htid;
                ideltid->id = delstate->ndeltids;
                istatus->idxoffnum = offnum;
                istatus->knowndeletable = false;
                istatus->promising = (firstpromising && p == 0) ||
                                   (lastpromising && p == nitem - 1);
                istatus->freespace = sizeof(ItemPointerData);

                ideltid++;
                istatus++;
                delstate->ndeltids++;
            }
        }
    }

    // Finalize interval if it contained duplicates
    if (dupinterval) {
        state->intervals[state->nintervals].nitems = state->nitems;
        state->nintervals++;
    }

    // Reset state for next interval
    state->nhtids = 0;
    state->nitems = 0;
    state->phystupsize = 0;
}
```