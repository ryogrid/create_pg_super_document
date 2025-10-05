# _bt_bottomupdel_pass

## Location
[src/backend/access/nbtree/nbtdedup.c:307-432](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtdedup.c#L307-L432)

## Overview
Performs bottom-up index deletion to remove duplicate index tuples and nearby tuples that correspond to deleted heap tuples, aiming to prevent unnecessary page splits caused by MVCC version churn.

## Definition

```c
bool
_bt_bottomupdel_pass(Relation rel, Buffer buf, Relation heapRel,
					 Size newitemsz)
```
## Detailed Description
This function implements bottom-up index deletion, a technique to remove index tuples whose corresponding heap tuples have been deleted or updated. The primary goal is to prevent page splits caused by MVCC version churn from UPDATE operations that don't logically modify indexed columns.

The function works by:
1. Scanning through page tuples and grouping duplicates into intervals
2. Passing these intervals to the table access method (tableam) to determine which heap TIDs are deletable
3. Physically deleting the identified tuples from the index page

The approach is qualitative rather than quantitative - it focuses on preventing unnecessary splits rather than maximizing the number of deleted tuples. The function returns advisory information about whether the operation was successful enough to avoid a page split.

## Parameters / Member Variables
- `rel`: The index relation being processed
- `buf`: Buffer containing the index page to process
- `heapRel`: The heap relation associated with the index
- `newitemsz`: Size of new item to be inserted (used for space target calculation)
## Dependencies
- Functions called/Symbols referenced:
  - : Initializes pending interval for duplicate detection
  - : Adds tuple's heap TIDs to current interval
  - : Finalizes interval and moves TIDs to delete state
  - : Asks tableam which TIDs are deletable and performs deletion
  - : Calculates remaining free space after deletion

- Called from (representative examples):
  - Called before  in insertion code paths to attempt bottom-up deletion first

## Notes and Other Information
- Returns true to indicate success (page split likely avoided), false to suggest deduplication should be attempted instead
- The space target for deletion is set to max(BLCKSZ/16, newitemsz) to ensure sufficient space is freed
- Even with zero promising tuples, the function may return true to avoid useless deduplication
- The function unconditionally returns true when no intervals are found to prevent deduplication
- Success threshold is based on freeing at least max(BLCKSZ/24, newitemsz) space
- This technique is particularly effective for workloads with frequent UPDATEs that don't change indexed columns

## Simplified Source

```c
bool _bt_bottomupdel_pass(Relation rel, Buffer buf, Relation heapRel, Size newitemsz) {
    Page page = BufferGetPage(buf);
    BTPageOpaque opaque = BTPageGetOpaque(page);
    BTDedupState state;
    TM_IndexDeleteOp delstate;
    bool neverdedup;
    int nkeyatts = IndexRelationGetNumberOfKeyAttributes(rel);

    // Include line pointer in size calculation
    newitemsz += sizeof(ItemIdData);

    // Initialize deduplication state for interval detection
    state = (BTDedupState) palloc(sizeof(BTDedupStateData));
    state->deduplicate = true;
    state->maxpostingsize = BLCKSZ; // Not really deduplicating
    state->htids = palloc(state->maxpostingsize);
    // ... initialize other state fields

    // Initialize tableam deletion state
    delstate.irel = rel;
    delstate.iblknum = BufferGetBlockNumber(buf);
    delstate.bottomup = true;
    delstate.bottomupfreespace = Max(BLCKSZ / 16, newitemsz);
    delstate.ndeltids = 0;
    delstate.deltids = palloc(MaxTIDsPerBTreePage * sizeof(TM_IndexDelete));
    delstate.status = palloc(MaxTIDsPerBTreePage * sizeof(TM_IndexStatus));

    // Scan page tuples and group duplicates into intervals
    OffsetNumber minoff = P_FIRSTDATAKEY(opaque);
    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);

    for (OffsetNumber offnum = minoff; offnum <= maxoff; offnum++) {
        ItemId itemid = PageGetItemId(page, offnum);
        IndexTuple itup = (IndexTuple) PageGetItem(page, itemid);

        if (offnum == minoff) {
            // Start first interval with this tuple
            _bt_dedup_start_pending(state, itup, offnum);
        }
        else if (_bt_keep_natts_fast(rel, state->base, itup) > nkeyatts &&
                 _bt_dedup_save_htid(state, itup)) {
            // Tuple matches current interval - add its TIDs
        }
        else {
            // Finalize current interval and move TIDs to delete state
            _bt_bottomupdel_finish_pending(page, state, &delstate);

            // Start new interval with this tuple
            _bt_dedup_start_pending(state, itup, offnum);
        }
    }

    // Finalize the last interval
    _bt_bottomupdel_finish_pending(page, state, &delstate);

    // Set flag to avoid deduplication if no intervals found
    neverdedup = (state->nintervals == 0);

    // Cleanup deduplication state
    pfree(state->htids);
    pfree(state);

    // Ask tableam which TIDs are deletable and perform deletion
    _bt_delitems_delete_check(rel, buf, heapRel, &delstate);

    // Cleanup deletion state
    pfree(delstate.deltids);
    pfree(delstate.status);

    // Return success if no intervals to avoid useless deduplication
    if (neverdedup)
        return true;

    // Return success if we freed enough space to avoid split
    return PageGetExactFreeSpace(page) >= Max(BLCKSZ / 24, newitemsz);
}
```