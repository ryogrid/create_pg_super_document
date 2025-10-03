# heap_xlog_prune_freeze

## Location
[src/backend/access/heap/heapam.c:9211-9362](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L9211-L9362)

## Overview
Replays XLOG_HEAP2_PRUNE_* WAL records during recovery, handling both tuple pruning and freezing operations on heap pages.

## Definition

```c
static void
heap_xlog_prune_freeze(XLogReaderState *record)
```
## Detailed Description
The  function is a critical WAL replay function that reconstructs the state of heap pages after prune and freeze operations during PostgreSQL recovery. It processes complex WAL records containing information about line pointer redirections, dead tuples, unused items, and tuple freezing plans.

The function handles multiple aspects of page recovery:
- Resolves recovery conflicts in Hot Standby mode using snapshot conflict horizons
- Manages different lock types (cleanup vs. exclusive) based on WAL record flags
- Deserializes prune and freeze information from WAL data
- Executes line pointer updates (redirections, marking dead/unused items)
- Applies tuple freezing plans to update transaction IDs and infomasks
- Updates the free space map when space is reclaimed

The function is designed to work with or without full-page images and ensures proper recovery semantics for vacuum operations.

## Parameters / Member Variables
- `*record`: XLogReaderState containing the WAL record to replay
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - [XLogRecGetBlockTag](../X/XLogRecGetBlockTag.md)
  - [XLogReadBufferForRedoExtended](../X/XLogReadBufferForRedoExtended.md)
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md)
  - [ResolveRecoveryConflictWithSnapshot](../R/ResolveRecoveryConflictWithSnapshot.md)
  - [heap_xlog_deserialize_prune_and_freeze](heap_xlog_deserialize_prune_and_freeze.md)
  - [heap_page_prune_execute](heap_page_prune_execute.md)
  - [heap_execute_freeze_tuple](heap_execute_freeze_tuple.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [PageSetLSN](../P/PageSetLSN.md)
  - [PageGetHeapFreeSpace](../P/PageGetHeapFreeSpace.md)
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
  - [XLogRecordPageWithFreeSpace](../X/XLogRecordPageWithFreeSpace.md)
  - [xl_heap_prune](../x/xl_heap_prune.md) (WAL record structure)
  - [xlhp_freeze_plan](../x/xlhp_freeze_plan.md)
  - [HeapTupleFreeze](../H/HeapTupleFreeze.md)
  - Various XLHP_* flags
  - BLK_NEEDS_REDO
  - RBM_NORMAL
- Called from:
  - [heap2_redo](heap2_redo.md)

## Notes and Other Information
- This function is static and only used internally within heapam.c for WAL replay
- Handles both pruning (removing dead tuples) and freezing (updating old transaction IDs) in a single operation
- Ensures proper recovery conflict handling in Hot Standby mode by checking snapshot conflict horizons
- Manages different buffer lock types based on whether cleanup locks are required
- Asserts that certain operations requiring tuple movement only occur with cleanup locks
- Processes variable-length WAL data containing offset arrays and freeze plans
- Updates the free space map only when space is actually reclaimed to maintain FSM accuracy
- Does not update page prunability hints during recovery, allowing natural hint bit updates during normal operation
- Supports partial page recovery when full-page images are not available
- Critical for maintaining data consistency during crash recovery and standby replay

## Simplified Source

```c
static void
heap_xlog_prune_freeze(XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    xl_heap_prune xlrec;
    Buffer buffer;
    RelFileLocator rlocator;
    BlockNumber blkno;
    XLogRedoAction action;

    // Parse WAL record header
    XLogRecGetBlockTag(record, 0, &rlocator, NULL, &blkno);
    memcpy(&xlrec, XLogRecGetData(record), SizeOfHeapPrune);

    // Handle recovery conflicts in Hot Standby mode
    if ((xlrec.flags & XLHP_HAS_CONFLICT_HORIZON) != 0) {
        TransactionId snapshot_conflict_horizon;
        // Extract conflict horizon from WAL data
        if (InHotStandby)
            ResolveRecoveryConflictWithSnapshot(snapshot_conflict_horizon,
                                               (xlrec.flags & XLHP_IS_CATALOG_REL) != 0,
                                               rlocator);
    }

    // Read the target page for redo
    action = XLogReadBufferForRedoExtended(record, 0, RBM_NORMAL,
                                         (xlrec.flags & XLHP_CLEANUP_LOCK) != 0,
                                         &buffer);

    if (action == BLK_NEEDS_REDO) {
        Page page = BufferGetPage(buffer);
        OffsetNumber *redirected, *nowdead, *nowunused;
        int nredirected, ndead, nunused, nplans;
        xlhp_freeze_plan *plans;
        OffsetNumber *frz_offsets;

        // Deserialize pruning and freezing data from WAL
        heap_xlog_deserialize_prune_and_freeze(
            XLogRecGetBlockData(record, 0, NULL), xlrec.flags,
            &nplans, &plans, &frz_offsets,
            &nredirected, &redirected,
            &ndead, &nowdead,
            &nunused, &nowunused);

        // Execute line pointer updates (redirect/dead/unused)
        if (nredirected > 0 || ndead > 0 || nunused > 0)
            heap_page_prune_execute(buffer,
                                  (xlrec.flags & XLHP_CLEANUP_LOCK) == 0,
                                  redirected, nredirected,
                                  nowdead, ndead,
                                  nowunused, nunused);

        // Apply freeze plans to tuples
        for (int p = 0; p < nplans; p++) {
            HeapTupleFreeze frz;
            frz.xmax = plans[p].xmax;
            frz.t_infomask2 = plans[p].t_infomask2;
            frz.t_infomask = plans[p].t_infomask;
            frz.frzflags = plans[p].frzflags;

            // Freeze each tuple in this plan
            for (int i = 0; i < plans[p].ntuples; i++) {
                OffsetNumber offset = *(frz_offsets++);
                ItemId lp = PageGetItemId(page, offset);
                HeapTupleHeader tuple = (HeapTupleHeader) PageGetItem(page, lp);
                heap_execute_freeze_tuple(tuple, &frz);
            }
        }

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }

    // Update FSM if space was reclaimed
    if (BufferIsValid(buffer)) {
        if (xlrec.flags & (XLHP_HAS_REDIRECTIONS | XLHP_HAS_DEAD_ITEMS | XLHP_HAS_NOW_UNUSED_ITEMS)) {
            Size freespace = PageGetHeapFreeSpace(BufferGetPage(buffer));
            UnlockReleaseBuffer(buffer);
            XLogRecordPageWithFreeSpace(rlocator, blkno, freespace);
        } else {
            UnlockReleaseBuffer(buffer);
        }
    }
}
```