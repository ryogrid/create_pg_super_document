# heap_xlog_insert

## Location
[src/backend/access/heap/heapam.c:9592-9710](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L9592-L9710)

## Overview
Replays XLOG_HEAP_INSERT WAL records during PostgreSQL recovery to restore tuple insertion operations and maintain proper page and visibility map state.

## Definition
```c
static void heap_xlog_insert(XLogReaderState *record)
```

## Detailed Description
This function handles the recovery of tuple insertion operations from WAL records during PostgreSQL crash recovery or standby replay. It reconstructs inserted tuples and places them at their original locations while maintaining data integrity.

Key operations include:

1. **Visibility Map Management**: Clears visibility map bits when insertions affect previously all-visible pages.

2. **Page Initialization**: Handles special case where the insertion creates the first tuple on a page, requiring full page initialization.

3. **Tuple Reconstruction**: Rebuilds the complete tuple from WAL data including header information and user data, setting appropriate transaction IDs and command IDs.

4. **Page Management**: Adds the reconstructed tuple to the page at the correct offset and updates page metadata.

5. **FSM Updates**: Updates the Free Space Map when page free space falls below 20% to maintain accurate free space tracking.

The function includes comprehensive validation and will panic if inconsistencies are detected during recovery.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record data for the insert operation, including the xl_heap_insert structure with insertion details

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData: Extract xl_heap_insert structure from WAL record
  - [XLogRecGetBlockTag](../X/XLogRecGetBlockTag.md): Get target relation and block information
  - [ItemPointerSetBlockNumber](../I/ItemPointerSetBlockNumber.md)/ItemPointerSetOffsetNumber: Set target tuple location
  - [CreateFakeRelcacheEntry](../C/CreateFakeRelcacheEntry.md)/FreeFakeRelcacheEntry: Temporary relation cache management
  - [visibilitymap_pin](../v/visibilitymap_pin.md)/visibilitymap_clear: Update visibility map when needed
  - XLogRecGetInfo: Check for special page initialization flag
  - [XLogInitBufferForRedo](../X/XLogInitBufferForRedo.md): Initialize buffer for page creation
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md): Read target page for redo operation
  - [PageInit](../P/PageInit.md): Initialize new page structure
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md): Validate insertion offset
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md): Extract tuple data from WAL record
  - HeapTupleHeaderSetXmin/HeapTupleHeaderSetCmin: Set transaction and command IDs
  - PageAddItem: Insert tuple into page at specified offset
  - [PageGetHeapFreeSpace](../P/PageGetHeapFreeSpace.md): Calculate remaining free space
  - [PageClearAllVisible](../P/PageClearAllVisible.md): Clear page visibility flag when needed
  - [XLogRecordPageWithFreeSpace](../X/XLogRecordPageWithFreeSpace.md): Update FSM for low free space pages

- Called from:
  - [heap_redo](heap_redo.md): Main heap WAL record replay dispatcher

## Notes and Other Information
- This is a static function exclusively used during WAL recovery operations
- Handles both regular insertions and first-tuple-on-page scenarios requiring page initialization
- Includes assertion that frozen tuple insertions are not supported in this code path
- Uses a union buffer structure to safely reconstruct tuples up to MaxHeapTupleSize
- Implements FSM update heuristic for pages with less than 20% free space remaining
- The function validates tuple placement and panics on offset number or tuple addition failures
- Essential for maintaining MVCC consistency during recovery operations
- Reconstructed tuples receive the transaction ID from the WAL record and FirstCommandId
- Target tuple ID (t_ctid) is set to the insertion location for new tuples
- Only updates FSM when actual redo is needed, not when pages are restored from full page images

## Simplified Source

```c
static void
heap_xlog_insert(XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    xl_heap_insert *xlrec = (xl_heap_insert *) XLogRecGetData(record);
    Buffer buffer;
    Page page;
    union {
        HeapTupleHeaderData hdr;
        char data[MaxHeapTupleSize];
    } tbuf;
    HeapTupleHeader htup;
    xl_heap_header xlhdr;
    uint32 newlen;
    Size freespace = 0;
    RelFileLocator target_locator;
    BlockNumber blkno;
    ItemPointerData target_tid;
    XLogRedoAction action;

    // Extract target location
    XLogRecGetBlockTag(record, 0, &target_locator, NULL, &blkno);
    ItemPointerSetBlockNumber(&target_tid, blkno);
    ItemPointerSetOffsetNumber(&target_tid, xlrec->offnum);

    // Clear visibility map if needed
    if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED) {
        Relation reln = CreateFakeRelcacheEntry(target_locator);
        Buffer vmbuffer = InvalidBuffer;

        visibilitymap_pin(reln, blkno, &vmbuffer);
        visibilitymap_clear(reln, blkno, vmbuffer, VISIBILITYMAP_VALID_BITS);
        ReleaseBuffer(vmbuffer);
        FreeFakeRelcacheEntry(reln);
    }

    // Initialize page if inserting first tuple
    if (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE) {
        buffer = XLogInitBufferForRedo(record, 0);
        page = BufferGetPage(buffer);
        PageInit(page, BufferGetPageSize(buffer), 0);
        action = BLK_NEEDS_REDO;
    } else {
        action = XLogReadBufferForRedo(record, 0, &buffer);
    }

    if (action == BLK_NEEDS_REDO) {
        Size datalen;
        char *data;

        page = BufferGetPage(buffer);

        // Validate insertion offset
        if (PageGetMaxOffsetNumber(page) + 1 < xlrec->offnum)
            elog(PANIC, "invalid max offset number");

        // Extract tuple data from WAL
        data = XLogRecGetBlockData(record, 0, &datalen);
        newlen = datalen - SizeOfHeapHeader;
        memcpy((char *) &xlhdr, data, SizeOfHeapHeader);
        data += SizeOfHeapHeader;

        // Reconstruct tuple
        htup = &tbuf.hdr;
        MemSet((char *) htup, 0, SizeofHeapTupleHeader);
        memcpy((char *) htup + SizeofHeapTupleHeader, data, newlen);
        newlen += SizeofHeapTupleHeader;

        // Set tuple header fields
        htup->t_infomask2 = xlhdr.t_infomask2;
        htup->t_infomask = xlhdr.t_infomask;
        htup->t_hoff = xlhdr.t_hoff;
        HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
        HeapTupleHeaderSetCmin(htup, FirstCommandId);
        htup->t_ctid = target_tid;

        // Insert tuple into page
        if (PageAddItem(page, (Item) htup, newlen, xlrec->offnum,
                       true, true) == InvalidOffsetNumber)
            elog(PANIC, "failed to add tuple");

        freespace = PageGetHeapFreeSpace(page);
        PageSetLSN(page, lsn);

        if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
            PageClearAllVisible(page);

        MarkBufferDirty(buffer);
    }

    if (BufferIsValid(buffer))
        UnlockReleaseBuffer(buffer);

    // Update FSM if page is running low on space
    if (action == BLK_NEEDS_REDO && freespace < BLCKSZ / 5)
        XLogRecordPageWithFreeSpace(target_locator, blkno, freespace);
}
```