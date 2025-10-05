# btree_xlog_delete

## Location
[src/backend/access/nbtree/nbtxlog.c:651-712](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtxlog.c#L651-L712)

## Overview
Replays WAL records for B-tree delete operations, handling both posting list updates and tuple deletions with hot standby conflict resolution.

## Definition

```c
static void
btree_xlog_delete(XLogReaderState *record)
```
## Detailed Description
This function handles the recovery/replay of B-tree delete operations from WAL records. B-tree delete operations remove tuples that are no longer visible to any running transactions, helping to maintain index efficiency and reclaim space.

The function is similar to btree_xlog_vacuum but includes additional logic for hot standby conflict resolution. When running on a hot standby server, it must check for potential conflicts with running queries before applying the changes, as deleted tuples might still be visible to some standby queries.

Key operations performed:
1. Resolves recovery conflicts with snapshot on hot standby (if applicable)
2. Processes posting list updates by calling btree_xlog_updates
3. Performs complete tuple deletions using PageIndexMultiDelete  
4. Clears the BTP_HAS_GARBAGE flag from the page
5. Updates the page LSN and marks the buffer dirty

The main difference from btree_xlog_vacuum is the conflict resolution step and the fact that it uses a regular buffer lock rather than a cleanup lock.

## Parameters / Member Variables
- `*record`: XLogReaderState containing the WAL record data for the delete operation
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - [XLogRecGetBlockTag](../X/XLogRecGetBlockTag.md)
  - [ResolveRecoveryConflictWithSnapshot](../R/ResolveRecoveryConflictWithSnapshot.md)
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md)
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
- Includes hot standby conflict resolution to prevent query conflicts
- Uses regular buffer locks (not cleanup locks) unlike btree_xlog_vacuum
- The xl_btree_delete record contains snapshot conflict horizon for standby conflict detection
- Processes both updated posting lists and completely deleted tuples
- Part of PostgreSQL's transaction visibility and hot standby replication system
- Critical for maintaining MVCC consistency during recovery on standby servers

## Simplified Source

```c
static void
btree_xlog_delete(XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    xl_btree_delete *xlrec = (xl_btree_delete *) XLogRecGetData(record);
    Buffer buffer;
    Page page;
    BTPageOpaque opaque;

    // Handle hot standby conflicts before updating page
    if (InHotStandby)
    {
        RelFileLocator rlocator;
        XLogRecGetBlockTag(record, 0, &rlocator, NULL, NULL);
        ResolveRecoveryConflictWithSnapshot(xlrec->snapshotConflictHorizon,
                                           xlrec->isCatalogRel,
                                           rlocator);
    }

    // Apply changes (uses regular lock, not cleanup lock)
    if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
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

        // Delete tuples completely
        if (xlrec->ndeleted > 0)
            PageIndexMultiDelete(page, (OffsetNumber *)ptr, xlrec->ndeleted);

        // Clear garbage flag
        opaque = BTPageGetOpaque(page);
        opaque->btpo_flags &= ~BTP_HAS_GARBAGE;

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }

    if (BufferIsValid(buffer))
        UnlockReleaseBuffer(buffer);
}
```