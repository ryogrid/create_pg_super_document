# heap_xlog_delete

## Location
[src/backend/access/heap/heapam.c:9519-9591](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L9519-L9591)

## Overview
Replays XLOG_HEAP_DELETE WAL records during PostgreSQL recovery to restore tuple deletion operations and maintain proper visibility map state.

## Definition
```c
static void heap_xlog_delete(XLogReaderState *record)
```

## Detailed Description
This function handles the recovery of tuple deletion operations from WAL records during PostgreSQL crash recovery or standby replay. It performs several key tasks:

1. **Visibility Map Management**: Updates the visibility map when the deletion affects page visibility, clearing appropriate bits when a page transitions from all-visible to having deleted tuples.

2. **Tuple Header Updates**: Modifies the deleted tuples header information including transaction IDs, command IDs, and various status flags to reflect the deletion state.

3. **Special Deletion Types**: Handles different types of deletions including super deletions (used in some optimization scenarios) and partition movement operations.

4. **Page Maintenance**: Marks the page as a candidate for pruning and updates page-level visibility flags as needed.

The function carefully validates the tuple location and panics if inconsistencies are detected, ensuring data integrity during recovery.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record data for the delete operation, including the xl_heap_delete structure with deletion details

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData: Extract xl_heap_delete structure from WAL record
  - [XLogRecGetBlockTag](../X/XLogRecGetBlockTag.md): Get target relation and block information
  - [ItemPointerSetBlockNumber](../I/ItemPointerSetBlockNumber.md)/ItemPointerSetOffsetNumber: Set target tuple location
  - [CreateFakeRelcacheEntry](../C/CreateFakeRelcacheEntry.md)/FreeFakeRelcacheEntry: Temporary relation cache management
  - [visibilitymap_pin](../v/visibilitymap_pin.md)/visibilitymap_clear: Update visibility map when needed
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md): Read target page for redo operation
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)/PageGetItemId: Access tuple within page
  - [PageGetItem](../P/PageGetItem.md): Get tuple data from page
  - [fix_infomask_from_infobits](../f/fix_infomask_from_infobits.md): Restore tuple header flags from compressed WAL data
  - HeapTupleHeaderSetXmax/HeapTupleHeaderSetXmin: Set transaction ID fields
  - HeapTupleHeaderSetCmax: Set command ID
  - HeapTupleHeaderClearHotUpdated: Clear HOT update flag
  - PageSetPrunable: Mark page for future pruning
  - [PageClearAllVisible](../P/PageClearAllVisible.md): Clear page visibility flag
  - HeapTupleHeaderSetMovedPartitions: Handle partition movement deletions

- Called from:
  - [heap_redo](heap_redo.md): Main heap WAL record replay dispatcher

## Notes and Other Information
- This is a static function exclusively used during WAL recovery operations
- Handles both regular deletions and special cases like super deletions and partition moves  
- Includes comprehensive validation with PANIC on tuple location inconsistencies
- The function distinguishes between different deletion types through XLH_DELETE_* flags
- Super deletions set xmin to InvalidTransactionId instead of setting xmax
- Partition movement deletions use special t_ctid handling via HeapTupleHeaderSetMovedPartitions
- Essential for maintaining MVCC consistency and visibility during recovery operations
- Updates both tuple-level and page-level metadata to ensure proper recovery state

## Simplified Source

```c
static void
heap_xlog_delete(XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    xl_heap_delete *xlrec = (xl_heap_delete *) XLogRecGetData(record);
    Buffer buffer;
    Page page;
    ItemId lp;
    HeapTupleHeader htup;
    BlockNumber blkno;
    RelFileLocator target_locator;
    ItemPointerData target_tid;

    // Extract target location from WAL record
    XLogRecGetBlockTag(record, 0, &target_locator, NULL, &blkno);
    ItemPointerSetBlockNumber(&target_tid, blkno);
    ItemPointerSetOffsetNumber(&target_tid, xlrec->offnum);

    // Clear visibility map if page was all-visible
    if (xlrec->flags & XLH_DELETE_ALL_VISIBLE_CLEARED) {
        Relation reln = CreateFakeRelcacheEntry(target_locator);
        Buffer vmbuffer = InvalidBuffer;

        visibilitymap_pin(reln, blkno, &vmbuffer);
        visibilitymap_clear(reln, blkno, vmbuffer, VISIBILITYMAP_VALID_BITS);
        ReleaseBuffer(vmbuffer);
        FreeFakeRelcacheEntry(reln);
    }

    // Apply deletion to heap page
    if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO) {
        page = BufferGetPage(buffer);

        // Validate tuple location
        if (PageGetMaxOffsetNumber(page) >= xlrec->offnum)
            lp = PageGetItemId(page, xlrec->offnum);

        if (PageGetMaxOffsetNumber(page) < xlrec->offnum || !ItemIdIsNormal(lp))
            elog(PANIC, "invalid lp");

        htup = (HeapTupleHeader) PageGetItem(page, lp);

        // Update tuple header for deletion
        htup->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
        htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;
        HeapTupleHeaderClearHotUpdated(htup);
        fix_infomask_from_infobits(xlrec->infobits_set,
                                 &htup->t_infomask, &htup->t_infomask2);

        // Set transaction ID based on deletion type
        if (!(xlrec->flags & XLH_DELETE_IS_SUPER))
            HeapTupleHeaderSetXmax(htup, xlrec->xmax);
        else
            HeapTupleHeaderSetXmin(htup, InvalidTransactionId);

        HeapTupleHeaderSetCmax(htup, FirstCommandId, false);

        // Mark page for pruning and update visibility
        PageSetPrunable(page, XLogRecGetXid(record));

        if (xlrec->flags & XLH_DELETE_ALL_VISIBLE_CLEARED)
            PageClearAllVisible(page);

        // Handle special deletion types
        if (xlrec->flags & XLH_DELETE_IS_PARTITION_MOVE)
            HeapTupleHeaderSetMovedPartitions(htup);
        else
            htup->t_ctid = target_tid;

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }

    if (BufferIsValid(buffer))
        UnlockReleaseBuffer(buffer);
}
```