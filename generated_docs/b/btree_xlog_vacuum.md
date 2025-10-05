# btree_xlog_vacuum

## Location
[src/backend/access/nbtree/nbtxlog.c:598-650](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtxlog.c#L598-L650)

## Overview
Replays WAL records for B-tree vacuum operations, handling both posting list updates and tuple deletions during recovery.

## Definition

```c
static void
btree_xlog_vacuum(XLogReaderState *record)
```
## Detailed Description
This function handles the recovery/replay of B-tree vacuum operations from WAL records. B-tree vacuum removes dead tuples and updates posting list tuples to remove dead heap TIDs, helping to reclaim space and maintain index efficiency.

The function processes two types of operations stored in the WAL record:
1. Updates to posting list tuples (removing dead heap TIDs)
2. Complete deletion of dead tuples from the page

It takes a cleanup lock (similar to the original btvacuumpage operation) to ensure exclusive access during recovery. The function processes updates first, then deletions, and finally clears the BTP_HAS_GARBAGE flag to indicate the page no longer contains dead items.

Key operations performed:
1. Acquires a cleanup lock on the target page
2. Processes posting list updates by calling btree_xlog_updates
3. Performs complete tuple deletions using PageIndexMultiDelete
4. Clears the BTP_HAS_GARBAGE flag from the page
5. Updates the page LSN and marks the buffer dirty

## Parameters / Member Variables
- `*record`: XLogReaderState containing the WAL record data for the vacuum operation
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - [XLogReadBufferForRedoExtended](../X/XLogReadBufferForRedoExtended.md)
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md)
  - [btree_xlog_updates](btree_xlog_updates.md)
  - [PageIndexMultiDelete](../P/PageIndexMultiDelete.md)
  - BTPageGetOpaque
  - [PageSetLSN](../P/PageSetLSN.md)
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
- Called from (representative examples):
  - [btree_redo](btree_redo.md)

## Notes and Other Information
- This is a static function used internally for B-tree WAL recovery
- Takes a cleanup lock (exclusive access) like the original vacuum operation
- The WAL record contains both updated and deleted tuple information in a specific layout
- The BTP_HAS_GARBAGE flag is cleared to indicate the page is clean after vacuum
- Part of PostgreSQL's vacuum system for maintaining B-tree index efficiency
- Critical for proper space reclamation and performance maintenance during recovery

## Simplified Source

```c
static void
btree_xlog_vacuum(XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    xl_btree_vacuum *xlrec = (xl_btree_vacuum *) XLogRecGetData(record);
    Buffer buffer;
    Page page;
    BTPageOpaque opaque;

    // Get cleanup lock and check if redo is needed
    if (XLogReadBufferForRedoExtended(record, 0, RBM_NORMAL, true, &buffer) == BLK_NEEDS_REDO)
    {
        char *ptr = XLogRecGetBlockData(record, 0, NULL);
        page = (Page) BufferGetPage(buffer);

        // Process posting list updates first
        if (xlrec->nupdated > 0)
        {
            OffsetNumber *updatedoffsets = (OffsetNumber *)(ptr + xlrec->ndeleted * sizeof(OffsetNumber));
            xl_btree_update *updates = (xl_btree_update *)((char *)updatedoffsets + xlrec->nupdated * sizeof(OffsetNumber));

            btree_xlog_updates(page, updatedoffsets, updates, xlrec->nupdated);
        }

        // Delete dead tuples
        if (xlrec->ndeleted > 0)
            PageIndexMultiDelete(page, (OffsetNumber *)ptr, xlrec->ndeleted);

        // Clear garbage flag - page is now clean
        opaque = BTPageGetOpaque(page);
        opaque->btpo_flags &= ~BTP_HAS_GARBAGE;

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }

    if (BufferIsValid(buffer))
        UnlockReleaseBuffer(buffer);
}
```