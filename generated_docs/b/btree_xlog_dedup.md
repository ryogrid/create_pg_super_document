# btree_xlog_dedup

## Location
[src/backend/access/nbtree/nbtxlog.c:464-556](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtxlog.c#L464-L556)

## Overview
Replays WAL (Write-Ahead Log) records for B-tree page deduplication operations during recovery or standby replay.

## Definition

```c
static void
btree_xlog_dedup(XLogReaderState *record)
```
## Detailed Description
This function handles the recovery/replay of B-tree deduplication operations from WAL records. B-tree deduplication is an optimization that combines multiple index tuples with identical key values into a single posting list tuple, reducing page space usage and improving performance.

The function reconstructs the deduplication state from the WAL record and applies the same deduplication logic that was performed during the original operation. It processes deduplication intervals stored in the WAL record to recreate the posting list tuples on the target page.

Key operations performed:
1. Reads the deduplication intervals from the WAL record
2. Initializes a BTDedupState structure to track the deduplication process  
3. Reconstructs the page by processing each tuple according to the intervals
4. Creates posting list tuples by combining tuples with identical keys
5. Clears any garbage collection flags if present
6. Updates the page LSN and marks the buffer dirty

## Parameters / Member Variables
- `*record`: XLogReaderState containing the WAL record data for the deduplication operation
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md)
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md)
  - BTPageGetOpaque
  - [_bt_dedup_start_pending](_bt_dedup_start_pending.md)
  - [_bt_dedup_save_htid](_bt_dedup_save_htid.md)
  - [_bt_dedup_finish_pending](_bt_dedup_finish_pending.md)
  - [PageGetTempPageCopySpecial](../P/PageGetTempPageCopySpecial.md)
  - [PageRestoreTempPage](../P/PageRestoreTempPage.md)
- Called from (representative examples):
  - [btree_redo](btree_redo.md)

## Notes and Other Information
- This is a static function used internally for B-tree WAL recovery
- The function carefully reconstructs the exact same deduplication state that existed during the original operation
- Includes assertions to verify that the reconstructed intervals match the original WAL record data
- Handles both leaf pages with data and internal pages with high keys
- Part of PostgreSQL's crash recovery and streaming replication system

## Simplified Source

```c
static void btree_xlog_dedup(XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    xl_btree_dedup *xlrec = (xl_btree_dedup *) XLogRecGetData(record);
    Buffer buf;

    if (XLogReadBufferForRedo(record, 0, &buf) == BLK_NEEDS_REDO) {
        char *ptr = XLogRecGetBlockData(record, 0, NULL);
        Page page = (Page) BufferGetPage(buf);
        BTPageOpaque opaque = BTPageGetOpaque(page);
        OffsetNumber offnum, minoff, maxoff;
        BTDedupState state;
        BTDedupInterval *intervals;
        Page newpage;

        // Initialize deduplication state
        state = (BTDedupState) palloc(sizeof(BTDedupStateData));
        state->deduplicate = true;
        state->maxpostingsize = BTMaxItemSize(page);
        state->htids = palloc(state->maxpostingsize);
        state->nhtids = 0;
        state->nitems = 0;
        state->nintervals = 0;

        // Set up page boundaries
        minoff = P_FIRSTDATAKEY(opaque);
        maxoff = PageGetMaxOffsetNumber(page);
        newpage = PageGetTempPageCopySpecial(page);

        // Copy high key if not rightmost page
        if (!P_RIGHTMOST(opaque)) {
            ItemId itemid = PageGetItemId(page, P_HIKEY);
            Size itemsz = ItemIdGetLength(itemid);
            IndexTuple item = (IndexTuple) PageGetItem(page, itemid);

            if (PageAddItem(newpage, (Item) item, itemsz, P_HIKEY,
                            false, false) == InvalidOffsetNumber)
                elog(ERROR, "deduplication failed to add highkey");
        }

        // Process deduplication intervals
        intervals = (BTDedupInterval *) ptr;
        for (offnum = minoff; offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
            ItemId itemid = PageGetItemId(page, offnum);
            IndexTuple itup = (IndexTuple) PageGetItem(page, itemid);

            if (offnum == minoff) {
                _bt_dedup_start_pending(state, itup, offnum);
            } else if (state->nintervals < xlrec->nintervals &&
                       state->baseoff == intervals[state->nintervals].baseoff &&
                       state->nitems < intervals[state->nintervals].nitems) {
                if (!_bt_dedup_save_htid(state, itup))
                    elog(ERROR, "deduplication failed to add heap tid");
            } else {
                _bt_dedup_finish_pending(newpage, state);
                _bt_dedup_start_pending(state, itup, offnum);
            }
        }

        // Finish final pending group
        _bt_dedup_finish_pending(newpage, state);

        // Clear garbage flag if present
        if (P_HAS_GARBAGE(opaque)) {
            BTPageOpaque nopaque = BTPageGetOpaque(newpage);
            nopaque->btpo_flags &= ~BTP_HAS_GARBAGE;
        }

        // Replace original page
        PageRestoreTempPage(newpage, page);
        PageSetLSN(page, lsn);
        MarkBufferDirty(buf);
    }

    if (BufferIsValid(buf))
        UnlockReleaseBuffer(buf);
}
```