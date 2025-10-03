# heap_xlog_update

## Location
[src/backend/access/heap/heapam.c:9858-10129](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L9858-L10129)

## Overview
Handles the replay of UPDATE and HOT_UPDATE operations during WAL (Write-Ahead Log) recovery by reconstructing the heap tuple update transaction from the logged information.

## Definition
```c
static void heap_xlog_update(XLogReaderState *record, bool hot_update)
```

## Detailed Description
The `heap_xlog_update` function is a critical component of PostgreSQL's crash recovery mechanism that processes UPDATE and HOT (Heap-Only Tuple) UPDATE operations from the WAL during database recovery. It reconstructs the state of both the old and new tuple versions by:

1. **Processing the old tuple**: Updates the old tuple's header information, sets the forward chain link (t_ctid) to point to the new tuple location, and marks it appropriately for HOT updates
2. **Reconstructing the new tuple**: Builds the new tuple data by combining prefix/suffix data from the old tuple with new data from the WAL record
3. **Managing visibility maps**: Clears visibility map bits when tuples are no longer all-visible
4. **Handling cross-page updates**: Properly manages updates that span different heap pages
5. **Maintaining consistency**: Ensures proper locking order and atomic operations during replay

The function supports space optimization techniques like prefix/suffix compression where unchanged portions of tuples are not logged but reconstructed from the original tuple.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record to be replayed
- `hot_update`: Boolean flag indicating whether this is a HOT (Heap-Only Tuple) update, which occurs within the same page

## Dependencies
- Functions called/Symbols referenced:
  - [XLogRecGetBlockTag](../X/XLogRecGetBlockTag.md), XLogRecGetBlockTagExtended (block information extraction)
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md), XLogInitBufferForRedo (buffer management during redo)
  - [visibilitymap_pin](../v/visibilitymap_pin.md), visibilitymap_clear (visibility map maintenance)
  - PageAddItem, PageSetLSN, PageSetPrunable (page-level operations)
  - HeapTupleHeaderSetXmin, HeapTupleHeaderSetXmax, HeapTupleHeaderSetCmin (tuple header management)
  - [fix_infomask_from_infobits](../f/fix_infomask_from_infobits.md) (tuple visibility state reconstruction)
- Called from (representative examples):
  - [heap_redo](heap_redo.md) (main heap WAL replay dispatcher)

## Notes and Other Information
- **Recovery Safety**: The function carefully manages buffer locking order to prevent deadlocks during recovery, though this is less critical during WAL replay than normal operations
- **Space Optimization**: Supports prefix and suffix compression (XLH_UPDATE_PREFIX_FROM_OLD, XLH_UPDATE_SUFFIX_FROM_OLD flags) to minimize WAL logging overhead
- **Visibility Management**: Handles clearing of visibility map bits for both old and new pages when tuples are no longer all-visible
- **FSM Updates**: Updates the Free Space Map when the new page becomes low on free space (less than 20%), but skips this for HOT updates since space will be reclaimed after pruning
- **Error Handling**: Contains several PANIC-level assertions for data consistency validation during recovery
- **HOT Update Handling**: Special processing for Heap-Only Tuple updates that occur within the same page, avoiding cross-page complexity

## Simplified Source

```c
static void
heap_xlog_update(XLogReaderState *record, bool hot_update)
{
    XLogRecPtr lsn = record->EndRecPtr;
    xl_heap_update *xlrec = (xl_heap_update *) XLogRecGetData(record);
    RelFileLocator rlocator;
    BlockNumber oldblk, newblk;
    ItemPointerData newtid;
    Buffer obuffer, nbuffer;
    Page page;
    OffsetNumber offnum;
    HeapTupleData oldtup;
    HeapTupleHeader htup;
    uint16 prefixlen = 0, suffixlen = 0;
    union {
        HeapTupleHeaderData hdr;
        char data[MaxHeapTupleSize];
    } tbuf;
    xl_heap_header xlhdr;
    uint32 newlen;
    Size freespace = 0;
    XLogRedoAction oldaction, newaction;

    // Extract target locations
    XLogRecGetBlockTag(record, 0, &rlocator, NULL, &newblk);
    if (XLogRecGetBlockTagExtended(record, 1, NULL, NULL, &oldblk, NULL)) {
        // Cross-page update
        Assert(!hot_update);
    } else {
        oldblk = newblk;  // Same page update
    }

    ItemPointerSet(&newtid, newblk, xlrec->new_offnum);

    // Clear old page visibility map if needed
    if (xlrec->flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED) {
        Relation reln = CreateFakeRelcacheEntry(rlocator);
        Buffer vmbuffer = InvalidBuffer;
        visibilitymap_pin(reln, oldblk, &vmbuffer);
        visibilitymap_clear(reln, oldblk, vmbuffer, VISIBILITYMAP_VALID_BITS);
        ReleaseBuffer(vmbuffer);
        FreeFakeRelcacheEntry(reln);
    }

    // Process old tuple
    oldaction = XLogReadBufferForRedo(record, (oldblk == newblk) ? 0 : 1, &obuffer);
    if (oldaction == BLK_NEEDS_REDO) {
        page = BufferGetPage(obuffer);
        offnum = xlrec->old_offnum;

        if (PageGetMaxOffsetNumber(page) >= offnum)
            ItemId lp = PageGetItemId(page, offnum);

        htup = (HeapTupleHeader) PageGetItem(page, lp);
        oldtup.t_data = htup;
        oldtup.t_len = ItemIdGetLength(lp);

        // Update old tuple header
        htup->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
        htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;

        if (hot_update)
            HeapTupleHeaderSetHotUpdated(htup);
        else
            HeapTupleHeaderClearHotUpdated(htup);

        fix_infomask_from_infobits(xlrec->old_infobits_set,
                                 &htup->t_infomask, &htup->t_infomask2);
        HeapTupleHeaderSetXmax(htup, xlrec->old_xmax);
        HeapTupleHeaderSetCmax(htup, FirstCommandId, false);
        htup->t_ctid = newtid;  // Forward chain link

        PageSetPrunable(page, XLogRecGetXid(record));
        if (xlrec->flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED)
            PageClearAllVisible(page);

        PageSetLSN(page, lsn);
        MarkBufferDirty(obuffer);
    }

    // Handle new page
    if (oldblk == newblk) {
        nbuffer = obuffer;
        newaction = oldaction;
    } else if (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE) {
        nbuffer = XLogInitBufferForRedo(record, 0);
        page = BufferGetPage(nbuffer);
        PageInit(page, BufferGetPageSize(nbuffer), 0);
        newaction = BLK_NEEDS_REDO;
    } else {
        newaction = XLogReadBufferForRedo(record, 0, &nbuffer);
    }

    // Clear new page visibility map if needed
    if (xlrec->flags & XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED) {
        Relation reln = CreateFakeRelcacheEntry(rlocator);
        Buffer vmbuffer = InvalidBuffer;
        visibilitymap_pin(reln, newblk, &vmbuffer);
        visibilitymap_clear(reln, newblk, vmbuffer, VISIBILITYMAP_VALID_BITS);
        ReleaseBuffer(vmbuffer);
        FreeFakeRelcacheEntry(reln);
    }

    // Reconstruct and insert new tuple
    if (newaction == BLK_NEEDS_REDO) {
        char *recdata;
        Size datalen, tuplen;

        recdata = XLogRecGetBlockData(record, 0, &datalen);
        page = BufferGetPage(nbuffer);

        // Handle prefix/suffix compression
        if (xlrec->flags & XLH_UPDATE_PREFIX_FROM_OLD) {
            memcpy(&prefixlen, recdata, sizeof(uint16));
            recdata += sizeof(uint16);
        }
        if (xlrec->flags & XLH_UPDATE_SUFFIX_FROM_OLD) {
            memcpy(&suffixlen, recdata, sizeof(uint16));
            recdata += sizeof(uint16);
        }

        memcpy((char *) &xlhdr, recdata, SizeOfHeapHeader);
        recdata += SizeOfHeapHeader;
        tuplen = datalen - (recdata - XLogRecGetBlockData(record, 0, NULL));

        // Reconstruct complete tuple
        htup = &tbuf.hdr;
        MemSet((char *) htup, 0, SizeofHeapTupleHeader);
        char *newp = (char *) htup + SizeofHeapTupleHeader;

        if (prefixlen > 0) {
            // Copy header + prefix from old + new data
            int len = xlhdr.t_hoff - SizeofHeapTupleHeader;
            memcpy(newp, recdata, len);
            newp += len; recdata += len;
            memcpy(newp, (char *) oldtup.t_data + oldtup.t_data->t_hoff, prefixlen);
            newp += prefixlen;
            memcpy(newp, recdata, tuplen - len);
            recdata += tuplen - len; newp += tuplen - len;
        } else {
            memcpy(newp, recdata, tuplen);
            newp += tuplen; recdata += tuplen;
        }

        if (suffixlen > 0)
            memcpy(newp, (char *) oldtup.t_data + oldtup.t_len - suffixlen, suffixlen);

        newlen = SizeofHeapTupleHeader + tuplen + prefixlen + suffixlen;
        htup->t_infomask2 = xlhdr.t_infomask2;
        htup->t_infomask = xlhdr.t_infomask;
        htup->t_hoff = xlhdr.t_hoff;
        HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
        HeapTupleHeaderSetCmin(htup, FirstCommandId);
        HeapTupleHeaderSetXmax(htup, xlrec->new_xmax);
        htup->t_ctid = newtid;

        // Insert new tuple
        offnum = PageAddItem(page, (Item) htup, newlen, xlrec->new_offnum, true, true);
        if (offnum == InvalidOffsetNumber)
            elog(PANIC, "failed to add tuple");

        if (xlrec->flags & XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED)
            PageClearAllVisible(page);

        freespace = PageGetHeapFreeSpace(page);
        PageSetLSN(page, lsn);
        MarkBufferDirty(nbuffer);
    }

    // Release buffers
    if (BufferIsValid(nbuffer) && nbuffer != obuffer)
        UnlockReleaseBuffer(nbuffer);
    if (BufferIsValid(obuffer))
        UnlockReleaseBuffer(obuffer);

    // Update FSM for non-HOT updates if space is low
    if (newaction == BLK_NEEDS_REDO && !hot_update && freespace < BLCKSZ / 5)
        XLogRecordPageWithFreeSpace(rlocator, newblk, freespace);
}
```