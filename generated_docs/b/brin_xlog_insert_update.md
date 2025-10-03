# brin_xlog_insert_update

## Location
[src/backend/access/brin/brin_xlog.c:46-123](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_xlog.c#L46-L123)

## Overview
A static helper function that handles the common parts of BRIN tuple insertion and update operations during WAL replay recovery.

## Definition
```c
static void brin_xlog_insert_update(XLogReaderState *record, xl_brin_insert *xlrec)
```

## Detailed Description
This function performs the common operations needed for both BRIN tuple insertions and updates during WAL replay. It handles two main tasks: inserting the new tuple into the regular BRIN page and updating the revmap (reverse mapping) to maintain the relationship between heap blocks and their corresponding BRIN tuples. The function can handle both cases where a new page needs to be initialized (when inserting the first tuple) and where tuples are added to existing pages. It ensures data integrity by properly setting LSNs and marking buffers as dirty.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record data
- `xlrec`: xl_brin_insert structure containing insert-specific information like heap block number, offset, and pages per range

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo: Gets record information flags
  - [XLogInitBufferForRedo](../X/XLogInitBufferForRedo.md): Initializes buffer for redo when creating new page
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md): Reads existing buffer for redo operations
  - [brin_page_init](brin_page_init.md): Initializes a new BRIN page
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md): Extracts block data from WAL record
  - PageAddItem: Adds tuple to page
  - [brinSetHeapBlockItemptr](brinSetHeapBlockItemptr.md): Updates revmap with new tuple location
  - XLOG_BRIN_INIT_PAGE: Flag indicating page initialization
  - [BrinTuple](../B/BrinTuple.md): BRIN tuple structure
- Called from (representative examples):
  - [brin_xlog_insert](brin_xlog_insert.md): BRIN insert replay function
  - [brin_xlog_update](brin_xlog_update.md): BRIN update replay function

## Notes and Other Information
- This is a static function shared between insert and update replay operations
- Handles both regular page updates and new page initialization based on WAL record flags
- Maintains both the main BRIN page data and the reverse mapping consistency
- Includes error handling with PANIC for invalid operations
- Located at src/backend/access/brin/brin_xlog.c:46-123
- No FSM (Free Space Map) updates are performed as noted in the comment

## Simplified Source

```c
static void brin_xlog_insert_update(XLogReaderState *record, xl_brin_insert *xlrec) {
    XLogRecPtr lsn = record->EndRecPtr;
    Buffer buffer;
    BlockNumber regpgno;
    Page page;
    XLogRedoAction action;

    // Handle page initialization if needed
    if (XLogRecGetInfo(record) & XLOG_BRIN_INIT_PAGE) {
        buffer = XLogInitBufferForRedo(record, 0);
        page = BufferGetPage(buffer);
        brin_page_init(page, BRIN_PAGETYPE_REGULAR);
        action = BLK_NEEDS_REDO;
    } else {
        action = XLogReadBufferForRedo(record, 0, &buffer);
    }

    regpgno = BufferGetBlockNumber(buffer);

    // Insert the tuple if redo is needed
    if (action == BLK_NEEDS_REDO) {
        OffsetNumber offnum;
        BrinTuple *tuple;
        Size tuplen;

        // Extract tuple from WAL record
        tuple = (BrinTuple *) XLogRecGetBlockData(record, 0, &tuplen);
        Assert(tuple->bt_blkno == xlrec->heapBlk);

        page = (Page) BufferGetPage(buffer);
        offnum = xlrec->offnum;

        // Validate offset and add tuple to page
        if (PageGetMaxOffsetNumber(page) + 1 < offnum)
            elog(PANIC, "brin_xlog_insert_update: invalid max offset number");

        offnum = PageAddItem(page, (Item) tuple, tuplen, offnum, true, false);
        if (offnum == InvalidOffsetNumber)
            elog(PANIC, "brin_xlog_insert_update: failed to add tuple");

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }

    if (BufferIsValid(buffer))
        UnlockReleaseBuffer(buffer);

    // Update the revmap
    action = XLogReadBufferForRedo(record, 1, &buffer);
    if (action == BLK_NEEDS_REDO) {
        ItemPointerData tid;

        ItemPointerSet(&tid, regpgno, xlrec->offnum);
        page = (Page) BufferGetPage(buffer);

        // Set heap block to tuple mapping in revmap
        brinSetHeapBlockItemptr(buffer, xlrec->pagesPerRange, xlrec->heapBlk, tid);
        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }

    if (BufferIsValid(buffer))
        UnlockReleaseBuffer(buffer);
}
```