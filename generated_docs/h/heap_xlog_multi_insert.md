# heap_xlog_multi_insert

## Location
[src/backend/access/heap/heapam.c:9711-9857](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L9711-L9857)

## Overview
Replays XLOG_HEAP2_MULTI_INSERT WAL records during PostgreSQL recovery to restore multiple tuple insertion operations in a single atomic operation, optimizing bulk insert performance.

## Definition
```c
static void heap_xlog_multi_insert(XLogReaderState *record)
```

## Detailed Description
This function handles the recovery of multi-tuple insertion operations from WAL records during PostgreSQL crash recovery or standby replay. It efficiently processes multiple tuples that were inserted in a single WAL record, which is commonly used for bulk operations like COPY, INSERT...VALUES with multiple rows, and other batch insertion scenarios.

Key operations include:

1. **Visibility Map Management**: Clears visibility map bits when insertions affect previously all-visible pages, or sets all-visible state for frozen tuple insertions.

2. **Page Initialization**: Handles cases where the multi-insert creates the first tuples on a page, requiring full page initialization.

3. **Batch Tuple Reconstruction**: Iterates through multiple tuples stored in the WAL record, reconstructing each with proper header information, transaction IDs, and placement offsets.

4. **Offset Management**: Handles two different offset strategies - sequential offsets for page initialization, or specific stored offsets for existing pages.

5. **Frozen Tuple Support**: Special handling for all-frozen tuple insertions that can immediately be marked as all-visible.

6. **FSM Updates**: Updates the Free Space Map when page free space falls below 20% after the multi-insert operation.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record data for the multi-insert operation, including the xl_heap_multi_insert structure with insertion details and tuple count

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData: Extract xl_heap_multi_insert structure from WAL record
  - [XLogRecGetBlockTag](../X/XLogRecGetBlockTag.md): Get target relation and block information
  - XLogRecGetInfo: Check for page initialization flag
  - [CreateFakeRelcacheEntry](../C/CreateFakeRelcacheEntry.md)/FreeFakeRelcacheEntry: Temporary relation cache management
  - [visibilitymap_pin](../v/visibilitymap_pin.md)/visibilitymap_clear: Update visibility map when needed
  - [XLogInitBufferForRedo](../X/XLogInitBufferForRedo.md): Initialize buffer for page creation
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md): Read target page for redo operation
  - [PageInit](../P/PageInit.md): Initialize new page structure
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md): Extract tuple data block from WAL record
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md): Validate insertion offsets
  - SHORTALIGN: Align tuple data pointers properly
  - HeapTupleHeaderSetXmin/HeapTupleHeaderSetCmin: Set transaction and command IDs
  - [ItemPointerSetBlockNumber](../I/ItemPointerSetBlockNumber.md)/ItemPointerSetOffsetNumber: Set tuple location
  - PageAddItem: Insert each tuple into page at specified offset
  - [PageGetHeapFreeSpace](../P/PageGetHeapFreeSpace.md): Calculate remaining free space
  - [PageClearAllVisible](../P/PageClearAllVisible.md): Clear page visibility flag when needed
  - [PageSetAllVisible](../P/PageSetAllVisible.md): Set page as all-visible for frozen insertions
  - [XLogRecordPageWithFreeSpace](../X/XLogRecordPageWithFreeSpace.md): Update FSM for low free space pages

- Called from:
  - [heap2_redo](heap2_redo.md): Heap2 WAL record replay dispatcher

## Notes and Other Information
- This is a static function exclusively used during WAL recovery operations
- Optimized for bulk insertion scenarios that benefit from batching multiple tuples in a single WAL record
- Includes assertion that XLH_INSERT_ALL_VISIBLE_CLEARED and XLH_INSERT_ALL_FROZEN_SET flags are mutually exclusive
- Handles both page initialization and existing page insertion scenarios
- Uses a union buffer structure to safely reconstruct tuples up to MaxHeapTupleSize
- For page initialization, tuples are placed sequentially starting from FirstOffsetNumber
- For existing pages, specific offsets are stored in the xlrec->offsets array
- Implements comprehensive validation including tuple data length verification
- Supports frozen tuple insertions that can immediately mark pages as all-visible
- Essential for maintaining performance during bulk data loading operations
- Only updates FSM when actual redo is needed and free space is below threshold
- The function panics on various validation failures to ensure data integrity during recovery

## Simplified Source

```c
static void
heap_xlog_multi_insert(XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    xl_heap_multi_insert *xlrec;
    RelFileLocator rlocator;
    BlockNumber blkno;
    Buffer buffer;
    Page page;
    union {
        HeapTupleHeaderData hdr;
        char data[MaxHeapTupleSize];
    } tbuf;
    HeapTupleHeader htup;
    uint32 newlen;
    Size freespace = 0;
    bool isinit = (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE) != 0;
    XLogRedoAction action;

    xlrec = (xl_heap_multi_insert *) XLogRecGetData(record);
    XLogRecGetBlockTag(record, 0, &rlocator, NULL, &blkno);

    // Clear visibility map if needed
    if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED) {
        Relation reln = CreateFakeRelcacheEntry(rlocator);
        Buffer vmbuffer = InvalidBuffer;

        visibilitymap_pin(reln, blkno, &vmbuffer);
        visibilitymap_clear(reln, blkno, vmbuffer, VISIBILITYMAP_VALID_BITS);
        ReleaseBuffer(vmbuffer);
        FreeFakeRelcacheEntry(reln);
    }

    // Initialize page if this is first tuple insertion
    if (isinit) {
        buffer = XLogInitBufferForRedo(record, 0);
        page = BufferGetPage(buffer);
        PageInit(page, BufferGetPageSize(buffer), 0);
        action = BLK_NEEDS_REDO;
    } else {
        action = XLogReadBufferForRedo(record, 0, &buffer);
    }

    if (action == BLK_NEEDS_REDO) {
        char *tupdata;
        char *endptr;
        Size len;

        // Extract tuple data from WAL record
        tupdata = XLogRecGetBlockData(record, 0, &len);
        endptr = tupdata + len;
        page = BufferGetPage(buffer);

        // Insert each tuple
        for (int i = 0; i < xlrec->ntuples; i++) {
            OffsetNumber offnum;
            xl_multi_insert_tuple *xlhdr;

            // Determine offset: sequential for init, stored for existing pages
            if (isinit)
                offnum = FirstOffsetNumber + i;
            else
                offnum = xlrec->offsets[i];

            if (PageGetMaxOffsetNumber(page) + 1 < offnum)
                elog(PANIC, "invalid max offset number");

            // Extract tuple header and data
            xlhdr = (xl_multi_insert_tuple *) SHORTALIGN(tupdata);
            tupdata = ((char *) xlhdr) + SizeOfMultiInsertTuple;

            newlen = xlhdr->datalen;
            htup = &tbuf.hdr;
            MemSet((char *) htup, 0, SizeofHeapTupleHeader);
            memcpy((char *) htup + SizeofHeapTupleHeader, tupdata, newlen);
            tupdata += newlen;

            // Set tuple header fields
            newlen += SizeofHeapTupleHeader;
            htup->t_infomask2 = xlhdr->t_infomask2;
            htup->t_infomask = xlhdr->t_infomask;
            htup->t_hoff = xlhdr->t_hoff;
            HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
            HeapTupleHeaderSetCmin(htup, FirstCommandId);
            ItemPointerSetBlockNumber(&htup->t_ctid, blkno);
            ItemPointerSetOffsetNumber(&htup->t_ctid, offnum);

            // Insert tuple into page
            offnum = PageAddItem(page, (Item) htup, newlen, offnum, true, true);
            if (offnum == InvalidOffsetNumber)
                elog(PANIC, "failed to add tuple");
        }

        freespace = PageGetHeapFreeSpace(page);
        PageSetLSN(page, lsn);

        // Update page visibility flags
        if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
            PageClearAllVisible(page);

        if (xlrec->flags & XLH_INSERT_ALL_FROZEN_SET)
            PageSetAllVisible(page);

        MarkBufferDirty(buffer);
    }

    if (BufferIsValid(buffer))
        UnlockReleaseBuffer(buffer);

    // Update FSM if page is running low on space
    if (action == BLK_NEEDS_REDO && freespace < BLCKSZ / 5)
        XLogRecordPageWithFreeSpace(rlocator, blkno, freespace);
}
```