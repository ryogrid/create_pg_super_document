# hash_xlog_delete

## Location
[src/backend/access/hash/hash_xlog.c:861-938](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hash_xlog.c#L861-L938)

## Overview
Replays a hash index delete operation during WAL recovery, removing index tuples from bucket or overflow pages while maintaining proper locking.

## Definition
static void hash_xlog_delete(XLogReaderState *record)

## Detailed Description
This function handles the replay of tuple deletion operations in hash indexes during PostgreSQL's crash recovery process. The function manages two buffers: a primary bucket buffer (for cleanup locking) and a delete buffer (the page from which tuples are being removed). It uses careful locking protocol to ensure that concurrent scans don't experience inconsistencies during replay. The function removes the specified tuples using PageIndexMultiDelete and optionally clears the dead tuple marking flag if requested. This operation is part of hash index maintenance operations like VACUUM or tuple cleanup.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record data for the delete operation

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - [XLogReadBufferForRedoExtended](../X/XLogReadBufferForRedoExtended.md)
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md)
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - [PageIndexMultiDelete](../P/PageIndexMultiDelete.md)
  - HashPageGetOpaque
  - [PageSetLSN](../P/PageSetLSN.md)
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
  - [BufferIsValid](../B/BufferIsValid.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
  - [xl_hash_delete](../x/xl_hash_delete.md)
  - XLogRedoAction
  - HashPageOpaque
  - RBM_NORMAL
  - BLK_NEEDS_REDO
  - LH_PAGE_HAS_DEAD_TUPLES
- Called from (representative examples):
  - [hash_redo](hash_redo.md)

## Notes and Other Information
- This is a static function used only within the hash WAL recovery module
- Implements cleanup locking protocol to prevent concurrent scan issues during replay
- Conditionally clears the LH_PAGE_HAS_DEAD_TUPLES flag based on the clear_dead_marking field in the WAL record
- Handles both primary bucket page deletions and overflow page deletions
- Part of PostgreSQL's hash index maintenance WAL recovery infrastructure
- The function ensures proper buffer management with validity checks and proper unlock/release order
- Related to hashbucketcleanup() operations for maintaining hash index integrity
- Supports bulk deletion of multiple tuples in a single operation through PageIndexMultiDelete

## Simplified Source

```c
static void
hash_xlog_delete(XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    xl_hash_delete *xldata = (xl_hash_delete *) XLogRecGetData(record);
    Buffer bucketbuf = InvalidBuffer;
    Buffer deletebuf;
    XLogRedoAction action;

    // Acquire cleanup lock on primary bucket page to prevent concurrent scans
    if (xldata->is_primary_bucket_page)
        action = XLogReadBufferForRedoExtended(record, 1, RBM_NORMAL, true, &deletebuf);
    else {
        // Get cleanup lock on bucket page first
        (void) XLogReadBufferForRedoExtended(record, 0, RBM_NORMAL, true, &bucketbuf);
        action = XLogReadBufferForRedo(record, 1, &deletebuf);
    }

    // Delete tuples from page
    if (action == BLK_NEEDS_REDO) {
        Page page = (Page) BufferGetPage(deletebuf);
        char *ptr = XLogRecGetBlockData(record, 1, &len);

        // Remove specified tuples
        if (len > 0) {
            OffsetNumber *unused = (OffsetNumber *) ptr;
            OffsetNumber *unend = (OffsetNumber *) ((char *) ptr + len);
            if ((unend - unused) > 0)
                PageIndexMultiDelete(page, unused, unend - unused);
        }

        // Clear dead marking flag if requested
        if (xldata->clear_dead_marking) {
            HashPageOpaque pageopaque = HashPageGetOpaque(page);
            pageopaque->hasho_flag &= ~LH_PAGE_HAS_DEAD_TUPLES;
        }

        PageSetLSN(page, lsn);
        MarkBufferDirty(deletebuf);
    }

    // Release buffers
    if (BufferIsValid(deletebuf))
        UnlockReleaseBuffer(deletebuf);
    if (BufferIsValid(bucketbuf))
        UnlockReleaseBuffer(bucketbuf);
}
```