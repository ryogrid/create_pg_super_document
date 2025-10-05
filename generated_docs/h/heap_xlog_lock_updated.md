# heap_xlog_lock_updated

## Location
[src/backend/access/heap/heapam.c:10237-10296](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L10237-L10296)

## Overview
Handles the replay of updated tuple locking operations during WAL recovery, specifically for locks that were applied to already-updated tuples in the update chain.

## Definition
```c
static void heap_xlog_lock_updated(XLogReaderState *record)
```

## Detailed Description
The `heap_xlog_lock_updated` function processes the replay of locking operations that were applied to updated tuples during PostgreSQL's WAL recovery. This function is specifically designed to handle locks placed on tuples that are part of an update chain - typically the newer versions of tuples that have been updated but still need to be locked by concurrent transactions.

The function differs from `heap_xlog_lock` in that it handles more complex locking scenarios where:
1. **Updated tuple locking**: Locks are applied to tuples that are part of an update chain
2. **Simplified processing**: Unlike regular lock operations, this doesn't handle lock-only semantics or ctid manipulation since the tuple is already part of an update chain
3. **Visibility management**: Still handles visibility map updates when locks affect frozen tuple status

Key operations include:
1. **Visibility map updates**: Clears "all frozen" bits when the lock affects tuple visibility
2. **Header modification**: Updates tuple infomask and infomask2 fields based on the lock operation
3. **Transaction tracking**: Sets the xmax field to identify the locking transaction

This function is typically called during the replay of lock operations that occurred on the newer versions of updated tuples, ensuring proper lock state reconstruction during recovery.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record with updated tuple lock information, including offset, flags, and locking transaction details

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData (extracts xl_heap_lock_updated structure from WAL record)
  - [XLogRecGetBlockTag](../X/XLogRecGetBlockTag.md) (retrieves block location information)
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md) (reads and locks target buffer for redo operations)
  - [visibilitymap_pin](../v/visibilitymap_pin.md), visibilitymap_clear (visibility map maintenance for frozen status)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md), PageGetItemId, PageGetItem (page-level tuple access functions)
  - [fix_infomask_from_infobits](../f/fix_infomask_from_infobits.md) (reconstructs tuple visibility state from logged information)
  - HeapTupleHeaderSetXmax (sets the locking transaction identifier)
- Called from (representative examples):
  - [heap2_redo](heap2_redo.md) (secondary heap WAL replay dispatcher for complex operations)

## Notes and Other Information
- **Update Chain Context**: Specifically designed for locking operations on tuples that are part of update chains, not standalone tuples
- **Simplified Logic**: More streamlined than `heap_xlog_lock` because it doesn't need to handle lock-only vs. update semantics
- **No Cmax Setting**: Unlike `heap_xlog_lock`, this function doesn't set cmax, suggesting these locks don't participate in command-level ordering
- **Visibility Map Handling**: Maintains consistency with visibility maps when locks affect tuple frozen status
- **Recovery Safety**: Includes PANIC-level validation to ensure data consistency during WAL replay
- **Transaction Coordination**: Properly records the locking transaction in xmax for proper concurrency control reconstruction

## Simplified Source

```c
static void heap_xlog_lock_updated(XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    xl_heap_lock_updated *xlrec = (xl_heap_lock_updated *) XLogRecGetData(record);
    Buffer buffer;
    Page page;
    HeapTupleHeader htup;

    // Clear visibility map if all-frozen flag was cleared during lock
    if (xlrec->flags & XLH_LOCK_ALL_FROZEN_CLEARED) {
        RelFileLocator rlocator;
        Buffer vmbuffer = InvalidBuffer;
        BlockNumber block;
        Relation reln;

        XLogRecGetBlockTag(record, 0, &rlocator, NULL, &block);
        reln = CreateFakeRelcacheEntry(rlocator);

        // Update visibility map to clear frozen status
        visibilitymap_pin(reln, block, &vmbuffer);
        visibilitymap_clear(reln, block, vmbuffer, VISIBILITYMAP_ALL_FROZEN);

        ReleaseBuffer(vmbuffer);
        FreeFakeRelcacheEntry(reln);
    }

    // Apply lock information to the tuple if redo is needed
    if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO) {
        page = BufferGetPage(buffer);

        // Locate the target tuple
        OffsetNumber offnum = xlrec->offnum;
        ItemId lp = PageGetItemId(page, offnum);

        if (PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsNormal(lp))
            elog(PANIC, "invalid lp");

        htup = (HeapTupleHeader) PageGetItem(page, lp);

        // Update tuple header with lock information
        htup->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
        htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;
        fix_infomask_from_infobits(xlrec->infobits_set, &htup->t_infomask, &htup->t_infomask2);
        HeapTupleHeaderSetXmax(htup, xlrec->xmax);

        // Mark page as modified
        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }

    if (BufferIsValid(buffer))
        UnlockReleaseBuffer(buffer);
}
```