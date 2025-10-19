# generic_redo

## Location
[src/backend/access/transam/generic_xlog.c:478-538](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/generic_xlog.c#L478-L538)

## Overview
Implements the redo logic for generic WAL records during crash recovery, reconstructing modified pages by applying delta changes and managing buffer lifecycle.

## Definition

```c
void
generic_redo(XLogReaderState *record)
```
## Detailed Description
generic_redo is the main redo function for generic WAL records in PostgreSQL's crash recovery system. It processes WAL records created by GenericXLogFinish, reconstructing the original page modifications during database recovery or standby replay.

The function iterates through all blocks referenced in the WAL record, reads each affected buffer, and applies delta changes using applyPageRedo. It handles the critical task of zeroing the "hole" between pd_lower and pd_upper in page headers to ensure consistency with the original logged operations. After applying changes, it sets the LSN, marks buffers dirty, and properly releases all acquired buffer locks.

## Parameters / Member Variables
- `*record`: XLogReaderState containing the WAL record to be replayed, including block references and delta data
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecMaxBlockId (gets maximum block ID in the record)
  - XLogRecHasBlockRef (checks if block reference exists)
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md) (reads buffer for redo operation)
  - [BufferGetPage](../B/BufferGetPage.md) (gets page from buffer)
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md) (extracts delta data from WAL record)
  - [applyPageRedo](../a/applyPageRedo.md) (applies delta changes to page)
  - [PageSetLSN](../P/PageSetLSN.md) (sets log sequence number on page)
  - [MarkBufferDirty](../M/MarkBufferDirty.md) (marks buffer as modified)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md) (releases buffer lock)
- Called from (representative examples):
  - No direct callers found in current analysis (typically called by WAL replay system)

## Notes and Other Information
- Part of PostgreSQL's generic WAL replay infrastructure (RM_GENERIC_ID resource manager)
- Supports up to MAX_GENERIC_XLOG_PAGES blocks per WAL record
- Includes Assert to protect against buffer array overflow
- Critical "hole zeroing" ensures replay consistency with original operations
- Proper buffer management prevents resource leaks during recovery
- Works in conjunction with GenericXLogFinish to provide crash recovery guarantees
- BLK_NEEDS_REDO action indicates when actual redo work is required
- Automatically handles cases where blocks don't need redo (e.g., already applied)

## Simplified Source

```c
void generic_redo(XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    Buffer buffers[MAX_GENERIC_XLOG_PAGES];
    uint8 block_id;

    // Process each block referenced in the WAL record
    for (block_id = 0; block_id <= XLogRecMaxBlockId(record); block_id++)
    {
        // Skip blocks not referenced in this record
        if (!XLogRecHasBlockRef(record, block_id)) {
            buffers[block_id] = InvalidBuffer;
            continue;
        }

        // Read buffer and determine if redo is needed
        XLogRedoAction action = XLogReadBufferForRedo(record, block_id, &buffers[block_id]);

        // Apply delta changes if block needs redo
        if (action == BLK_NEEDS_REDO) {
            Page page = BufferGetPage(buffers[block_id]);

            // Get delta data and apply changes
            Size blockDeltaSize;
            char *blockDelta = XLogRecGetBlockData(record, block_id, &blockDeltaSize);
            applyPageRedo(page, blockDelta, blockDeltaSize);

            // Zero the hole between pd_lower and pd_upper for consistency
            PageHeader pageHeader = (PageHeader) page;
            memset(page + pageHeader->pd_lower, 0,
                   pageHeader->pd_upper - pageHeader->pd_lower);

            // Update LSN and mark as dirty
            PageSetLSN(page, lsn);
            MarkBufferDirty(buffers[block_id]);
        }
    }

    // Release all acquired buffers
    for (block_id = 0; block_id <= XLogRecMaxBlockId(record); block_id++) {
        if (BufferIsValid(buffers[block_id]))
            UnlockReleaseBuffer(buffers[block_id]);
    }
}
```