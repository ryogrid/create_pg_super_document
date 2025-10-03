# heap_xlog_lock

## Location
[src/backend/access/heap/heapam.c:10166-10236](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L10166-L10236)

## Overview
Handles the replay of tuple locking operations during WAL recovery by updating tuple header information to reflect the lock state without modifying tuple data.

## Definition
```c
static void heap_xlog_lock(XLogReaderState *record)
```

## Detailed Description
The `heap_xlog_lock` function processes tuple locking operations during PostgreSQL's WAL recovery. This function is responsible for replaying locks that were placed on tuples during normal database operation. Unlike UPDATE operations, locking operations modify only the tuple header metadata (specifically the infomask fields and xmax) without changing the tuple's actual data content.

The function performs several key operations:
1. **Visibility map management**: Clears the "all frozen" bit in the visibility map if the lock operation affects tuple visibility
2. **Header updates**: Modifies the tuple's infomask and infomask2 fields to reflect the lock state
3. **Lock-only handling**: For lock-only operations (not updates), ensures the tuple's ctid points to itself and clears HOT update flags
4. **Transaction information**: Sets the appropriate xmax (locking transaction) and cmax values

The lock replay ensures that the tuple's visibility and locking state are correctly restored during recovery, maintaining consistency for concurrent access patterns.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record with lock operation details, including target tuple offset, lock flags, and transaction information

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData (extracts xl_heap_lock structure from WAL record)
  - [XLogRecGetBlockTag](../X/XLogRecGetBlockTag.md) (retrieves block information from WAL record)
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md) (reads and locks target buffer for redo)
  - [visibilitymap_pin](../v/visibilitymap_pin.md), visibilitymap_clear (visibility map maintenance for frozen tuples)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md), PageGetItemId, PageGetItem (page-level tuple access)
  - [fix_infomask_from_infobits](../f/fix_infomask_from_infobits.md) (reconstructs tuple visibility state from logged bits)
  - HeapTupleHeaderSetXmax, HeapTupleHeaderSetCmax (tuple header transaction info)
  - HEAP_XMAX_IS_LOCKED_ONLY (macro to check if operation is lock-only)
- Called from (representative examples):
  - [heap_redo](heap_redo.md) (main heap WAL replay dispatcher)

## Notes and Other Information
- **Lock-Only Operations**: Distinguishes between pure locking operations and lock-for-update operations using the HEAP_XMAX_IS_LOCKED_ONLY macro
- **Visibility Map Impact**: When locks affect tuple visibility (clearing frozen status), the visibility map must be updated accordingly
- **HOT Update Interaction**: For lock-only operations, clears HOT update flags and ensures self-referencing ctid to maintain tuple chain integrity
- **Transaction Consistency**: Properly sets xmax to the locking transaction and cmax for command ordering within transactions
- **Error Handling**: Includes PANIC-level validation to ensure tuple consistency during recovery
- **Metadata Focus**: Unlike update operations, this function only modifies tuple header metadata, not the tuple data itself

## Simplified Source

```c
static void
heap_xlog_lock(XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    xl_heap_lock *xlrec = (xl_heap_lock *) XLogRecGetData(record);
    Buffer buffer;
    Page page;
    OffsetNumber offnum;
    ItemId lp;
    HeapTupleHeader htup;

    // Clear visibility map if frozen status is affected
    if (xlrec->flags & XLH_LOCK_ALL_FROZEN_CLEARED) {
        RelFileLocator rlocator;
        Buffer vmbuffer = InvalidBuffer;
        BlockNumber block;
        Relation reln;

        XLogRecGetBlockTag(record, 0, &rlocator, NULL, &block);
        reln = CreateFakeRelcacheEntry(rlocator);

        visibilitymap_pin(reln, block, &vmbuffer);
        visibilitymap_clear(reln, block, vmbuffer, VISIBILITYMAP_ALL_FROZEN);

        ReleaseBuffer(vmbuffer);
        FreeFakeRelcacheEntry(reln);
    }

    if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO) {
        page = BufferGetPage(buffer);
        offnum = xlrec->offnum;

        // Validate tuple location
        if (PageGetMaxOffsetNumber(page) >= offnum)
            lp = PageGetItemId(page, offnum);

        if (PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsNormal(lp))
            elog(PANIC, "invalid lp");

        htup = (HeapTupleHeader) PageGetItem(page, lp);

        // Update tuple header for lock
        htup->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
        htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;
        fix_infomask_from_infobits(xlrec->infobits_set,
                                 &htup->t_infomask, &htup->t_infomask2);

        // Handle lock-only operations
        if (HEAP_XMAX_IS_LOCKED_ONLY(htup->t_infomask)) {
            HeapTupleHeaderClearHotUpdated(htup);
            // Make sure t_ctid points to self for lock-only
            ItemPointerSet(&htup->t_ctid,
                          BufferGetBlockNumber(buffer),
                          offnum);
        }

        HeapTupleHeaderSetXmax(htup, xlrec->xmax);
        HeapTupleHeaderSetCmax(htup, FirstCommandId, false);

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }

    if (BufferIsValid(buffer))
        UnlockReleaseBuffer(buffer);
}
```