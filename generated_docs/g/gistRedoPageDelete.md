# gistRedoPageDelete

## Location
[src/backend/access/gist/gistxlog.c:342-375](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistxlog.c#L342-L375)

## Overview
Replays the deletion of a GiST index page during WAL recovery, marking the deleted page and removing the corresponding downlink from its parent page.

## Definition

```c
static void
gistRedoPageDelete(XLogReaderState *record)
```
## Detailed Description
This function handles the redo operation for GiST page deletion during WAL recovery. It processes the  WAL record to restore the database state after a page deletion operation. The function operates on two buffers: the leaf page being deleted and its parent page that contains the downlink to be removed.

The operation involves two main steps:
1. Mark the leaf page as deleted by setting the deletion XID
2. Remove the downlink tuple from the parent page at the specified offset

Both pages are only modified if they need redo (determined by LSN comparison), ensuring idempotent recovery operations.

## Parameters / Member Variables
- `*record`: XLogReaderState pointer containing the WAL record data and metadata for the page deletion operation
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData: Extract WAL record data
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md): Read buffers and determine if redo is needed
  - BLK_NEEDS_REDO: Constant indicating buffer needs redo
  - [GistPageSetDeleted](../G/GistPageSetDeleted.md): Mark page as deleted with deletion XID
  - [PageIndexTupleDelete](../P/PageIndexTupleDelete.md): Remove tuple from page at specified offset
  - [BufferGetPage](../B/BufferGetPage.md): Get page from buffer
  - [PageSetLSN](../P/PageSetLSN.md): Set page LSN
  - [MarkBufferDirty](../M/MarkBufferDirty.md): Mark buffer as dirty
  - [BufferIsValid](../B/BufferIsValid.md): Check if buffer is valid
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md): Unlock and release buffer
- Called from:
  - [gist_redo](gist_redo.md): Main GiST WAL redo dispatcher

## Notes and Other Information
- This is a static function used internally within the GiST WAL recovery system
- The function handles both the leaf page (buffer 0) and parent page (buffer 1) as recorded in the WAL
- Proper buffer management ensures resources are cleaned up regardless of whether redo was needed
- The deletion XID stored in the WAL record is used to mark when the page was deleted, supporting MVCC visibility rules

## Simplified Source

```c
static void gistRedoPageDelete(XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    gistxlogPageDelete *xldata = (gistxlogPageDelete *) XLogRecGetData(record);
    Buffer parentBuffer;
    Buffer leafBuffer;

    // Mark leaf page as deleted if redo needed
    if (XLogReadBufferForRedo(record, 0, &leafBuffer) == BLK_NEEDS_REDO)
    {
        Page page = (Page) BufferGetPage(leafBuffer);

        GistPageSetDeleted(page, xldata->deleteXid);
        PageSetLSN(page, lsn);
        MarkBufferDirty(leafBuffer);
    }

    // Remove downlink from parent page if redo needed
    if (XLogReadBufferForRedo(record, 1, &parentBuffer) == BLK_NEEDS_REDO)
    {
        Page page = (Page) BufferGetPage(parentBuffer);

        PageIndexTupleDelete(page, xldata->downlinkOffset);
        PageSetLSN(page, lsn);
        MarkBufferDirty(parentBuffer);
    }

    // Clean up buffers
    if (BufferIsValid(parentBuffer))
        UnlockReleaseBuffer(parentBuffer);
    if (BufferIsValid(leafBuffer))
        UnlockReleaseBuffer(leafBuffer);
}
```