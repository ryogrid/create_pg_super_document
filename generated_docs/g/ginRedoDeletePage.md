# ginRedoDeletePage

## Location
[src/backend/access/gin/ginxlog.c:477-527](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginxlog.c#L477-L527)

## Overview
Replays GIN data page deletion operations during WAL recovery, updating sibling links and marking pages as deleted.

## Definition

```c
static void
ginRedoDeletePage(XLogReaderState *record)
```
## Detailed Description
ginRedoDeletePage is a WAL recovery function that replays GIN (Generalized Inverted Index) data page deletion operations from transaction log records. This function handles the complex process of removing a data page from the GIN index structure while maintaining consistency across multiple related pages.

The deletion process involves three pages:
1. **Deleted page** (dbuffer): The page being removed, marked as deleted with the transaction ID
2. **Left sibling page** (lbuffer): Updated to skip over the deleted page by updating its rightlink pointer  
3. **Parent page** (pbuffer): Updated to remove the posting item that pointed to the deleted page

The function carefully manages locking order to prevent deadlocks, specifically locking the left page first to avoid conflicts with the ginStepRight() function that traverses pages from left to right.

Key functionality:
- Updates left sibling's rightlink to skip the deleted page
- Marks the target page as deleted with the appropriate transaction ID
- Removes the posting item from the parent page that referenced the deleted page
- Maintains proper locking order to prevent deadlocks

## Parameters / Member Variables
- `*record`: XLogReaderState pointer containing the WAL record being replayed, including deletion metadata and affected page references
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - GinPageIsData
  - GinPageGetOpaque
  - GinPageSetDeleted
  - GinPageSetDeleteXid
  - GinPageIsLeaf
  - [GinPageDeletePostingItem](../G/GinPageDeletePostingItem.md)
  - [PageSetLSN](../P/PageSetLSN.md)
  - [MarkBufferDirty](../M/MarkBufferDirty.md)
  - [BufferIsValid](../B/BufferIsValid.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)

- Data structures used:
  - [ginxlogDeletePage](ginxlogDeletePage.md)

- Constants used:
  - BLK_NEEDS_REDO

- Called from:
  - [gin_redo](gin_redo.md)

## Notes and Other Information
- Handles three related pages in a specific order: left sibling first, then deleted page, then parent page
- Uses careful locking order (left page first) to prevent deadlocks with ginStepRight() function
- All affected pages must be data pages (verified with assertions)
- The parent page must not be a leaf page (only internal pages have posting items to delete)
- Transaction ID is recorded on the deleted page for proper visibility handling
- Part of PostgreSQL's GIN index deletion recovery mechanism ensuring structural consistency
- The ginxlogDeletePage structure contains rightLink, deleteXid, and parentOffset fields needed for recovery

## Simplified Source

```c
static void
ginRedoDeletePage(XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    ginxlogDeletePage *data = (ginxlogDeletePage *) XLogRecGetData(record);
    Buffer dbuffer, pbuffer, lbuffer;
    Page page;

    // Lock left page first to prevent deadlock with ginStepRight()
    if (XLogReadBufferForRedo(record, 2, &lbuffer) == BLK_NEEDS_REDO) {
        // Update left sibling's rightlink to skip deleted page
        page = BufferGetPage(lbuffer);
        GinPageGetOpaque(page)->rightlink = data->rightLink;
        PageSetLSN(page, lsn);
        MarkBufferDirty(lbuffer);
    }

    // Mark the target page as deleted
    if (XLogReadBufferForRedo(record, 0, &dbuffer) == BLK_NEEDS_REDO) {
        page = BufferGetPage(dbuffer);
        GinPageSetDeleted(page);
        GinPageSetDeleteXid(page, data->deleteXid);
        PageSetLSN(page, lsn);
        MarkBufferDirty(dbuffer);
    }

    // Remove posting item from parent page
    if (XLogReadBufferForRedo(record, 1, &pbuffer) == BLK_NEEDS_REDO) {
        page = BufferGetPage(pbuffer);
        GinPageDeletePostingItem(page, data->parentOffset);
        PageSetLSN(page, lsn);
        MarkBufferDirty(pbuffer);
    }

    // Release all buffers
    if (BufferIsValid(lbuffer))
        UnlockReleaseBuffer(lbuffer);
    if (BufferIsValid(pbuffer))
        UnlockReleaseBuffer(pbuffer);
    if (BufferIsValid(dbuffer))
        UnlockReleaseBuffer(dbuffer);
}
```