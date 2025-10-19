# hash_xlog_move_page_contents

## Location
[src/backend/access/hash/hash_xlog.c:501-626](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hash_xlog.c#L501-L626)

## Overview
Replays the movement of page contents during a hash index squeeze operation, transferring index tuples from one page to another while maintaining proper locking.

## Definition
static void hash_xlog_move_page_contents(XLogReaderState *record)

## Detailed Description
This function handles the replay of page content movement during hash index squeeze operations. A squeeze operation occurs when hash index pages need to be consolidated to reclaim space. The function manages three buffers: a primary bucket buffer (for locking), a write buffer (destination for moved tuples), and a delete buffer (source of tuples to be moved). The operation requires careful coordination to ensure that concurrent scans don't miss records or see duplicates. The function first acquires cleanup locks, then adds tuples to the destination page, removes them from the source page, and finally releases all buffers in the proper order.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record data for the page content movement operation

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - [XLogReadBufferForRedoExtended](../X/XLogReadBufferForRedoExtended.md)
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md)
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - IndexTupleSize
  - PageAddItem
  - [PageIndexMultiDelete](../P/PageIndexMultiDelete.md)
  - [PageSetLSN](../P/PageSetLSN.md)
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
  - [BufferIsValid](../B/BufferIsValid.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
  - [xl_hash_move_page_contents](../x/xl_hash_move_page_contents.md)
  - XLogRedoAction
  - RBM_NORMAL
  - BLK_NEEDS_REDO
  - InvalidOffsetNumber
  - Item
- Called from (representative examples):
  - [hash_redo](hash_redo.md)

## Notes and Other Information
- This is a static function used only within the hash WAL recovery module
- Implements careful locking protocol to prevent concurrent scan issues during replay
- Handles both addition of tuples to destination page and deletion from source page
- Uses cleanup locks on primary bucket page to ensure exclusive access during operation
- Part of PostgreSQL's hash index squeeze operation WAL recovery infrastructure
- Includes assertion checks to verify that the number of inserted tuples matches the WAL record expectations
- Buffer management is done in specific order to maintain lock hierarchy and prevent deadlocks

## Simplified Source

```c
static void
hash_xlog_move_page_contents(XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    xl_hash_move_page_contents *xldata = (xl_hash_move_page_contents *) XLogRecGetData(record);
    Buffer bucketbuf = InvalidBuffer;
    Buffer writebuf = InvalidBuffer;
    Buffer deletebuf = InvalidBuffer;
    XLogRedoAction action;

    // Acquire cleanup lock on primary bucket page to prevent concurrent scans
    if (xldata->is_prim_bucket_same_wrt)
        action = XLogReadBufferForRedoExtended(record, 1, RBM_NORMAL, true, &writebuf);
    else {
        // Get cleanup lock on bucket page first
        (void) XLogReadBufferForRedoExtended(record, 0, RBM_NORMAL, true, &bucketbuf);
        action = XLogReadBufferForRedo(record, 1, &writebuf);
    }

    // Add tuples to destination page
    if (action == BLK_NEEDS_REDO) {
        Page writepage = (Page) BufferGetPage(writebuf);
        char *data = XLogRecGetBlockData(record, 1, &datalen);
        uint16 ninserted = 0;

        if (xldata->ntups > 0) {
            OffsetNumber *towrite = (OffsetNumber *) data;
            data += sizeof(OffsetNumber) * xldata->ntups;

            // Add each tuple to destination page
            while (data - begin < datalen) {
                IndexTuple itup = (IndexTuple) data;
                Size itemsz = MAXALIGN(IndexTupleSize(itup));
                data += itemsz;

                if (PageAddItem(writepage, (Item) itup, itemsz, towrite[ninserted], false, false) == InvalidOffsetNumber)
                    elog(ERROR, "failed to add item to hash index page");
                ninserted++;
            }
        }

        PageSetLSN(writepage, lsn);
        MarkBufferDirty(writebuf);
    }

    // Remove tuples from source page
    if (XLogReadBufferForRedo(record, 2, &deletebuf) == BLK_NEEDS_REDO) {
        Page page = (Page) BufferGetPage(deletebuf);
        char *ptr = XLogRecGetBlockData(record, 2, &len);

        if (len > 0) {
            OffsetNumber *unused = (OffsetNumber *) ptr;
            OffsetNumber *unend = (OffsetNumber *) ((char *) ptr + len);
            if ((unend - unused) > 0)
                PageIndexMultiDelete(page, unused, unend - unused);
        }

        PageSetLSN(page, lsn);
        MarkBufferDirty(deletebuf);
    }

    // Release all buffers in proper order
    if (BufferIsValid(deletebuf))
        UnlockReleaseBuffer(deletebuf);
    if (BufferIsValid(writebuf))
        UnlockReleaseBuffer(writebuf);
    if (BufferIsValid(bucketbuf))
        UnlockReleaseBuffer(bucketbuf);
}
```