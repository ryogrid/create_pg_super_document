# heap_abort_speculative

## Location
[src/backend/access/heap/heapam.c:6129-6307](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L6129-L6307)

## Overview
Kills a speculatively inserted tuple by marking it as immediately dead, preventing unprincipled deadlocks in high-concurrency scenarios.

## Definition
```c
void heap_abort_speculative(Relation relation, ItemPointer tid)
```

## Detailed Description
This function aborts a speculative insertion by making the tuple immediately visible as dead to all transactions, including the inserting transaction itself. The key operations include:

1. Setting the tuple's xmin to InvalidTransactionId, making it immediately invisible
2. Clearing the speculative insertion token from t_ctid 
3. Setting up page pruning hints for future cleanup
4. Logging the operation via WAL as a delete operation
5. Handling any associated TOAST data cleanup

The function prevents unprincipled deadlocks that could occur when multiple backends attempt speculative insertions of duplicate keys. By immediately marking failed speculative insertions as dead, other backends don't need to wait for the entire transaction to complete.

## Parameters / Member Variables
- `relation`: The heap relation containing the speculative tuple to abort
- `tid`: ItemPointer identifying the location of the speculative tuple to be killed

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTransactionId](../G/GetCurrentTransactionId.md)
  - [ItemPointerIsValid](../I/ItemPointerIsValid.md)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [ReadBuffer](../R/ReadBuffer.md)
  - [PageIsAllVisible](../P/PageIsAllVisible.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - ItemIdIsNormal
  - HeapTupleHeaderIsSpeculative
  - [IsToastRelation](../I/IsToastRelation.md)
  - HeapTupleHeaderIsHeapOnly
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - PageSetPrunable
  - HeapTupleHeaderSetXmin
  - [compute_infobits](../c/compute_infobits.md)
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md)
  - [XLogInsert](../X/XLogInsert.md)
  - HeapTupleHasExternal
  - [heap_toast_delete](heap_toast_delete.md)
  - [ReleaseBuffer](../R/ReleaseBuffer.md)
  - [pgstat_count_heap_delete](../p/pgstat_count_heap_delete.md)
- Called from (representative examples):
  - [toast_delete_datum](../t/toast_delete_datum.md)
  - [heapam_tuple_complete_speculative](heapam_tuple_complete_speculative.md)
  - HeapScanIsValid (indirect reference)

## Notes and Other Information
- Uses WAL records identical to heap_delete() for recovery consistency
- Performs extensive validation to ensure the tuple is speculative and inserted by the current transaction
- Prevents unprincipled deadlocks by making failed speculative insertions immediately visible as dead
- Handles TOAST data cleanup for tuples with external storage
- Updates heap statistics by counting the deletion
- Sets pruning hints using TransactionXmin or relation's relfrozenxid for future cleanup efficiency
- Never requires catalog invalidation since catalogs don't support speculative insertion

## Simplified Source

```c
void
heap_abort_speculative(Relation relation, ItemPointer tid)
{
    TransactionId xid = GetCurrentTransactionId();

    // Read the page and get exclusive lock
    BlockNumber block = ItemPointerGetBlockNumber(tid);
    Buffer buffer = ReadBuffer(relation, block);
    Page page = BufferGetPage(buffer);
    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

    // Get tuple data
    ItemId lp = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));
    HeapTupleData tp;
    tp.t_data = (HeapTupleHeader) PageGetItem(page, lp);
    tp.t_len = ItemIdGetLength(lp);
    tp.t_self = *tid;

    // Validate this is our speculative tuple
    if (tp.t_data->t_choice.t_heap.t_xmin != xid)
        elog(ERROR, "attempted to kill a tuple inserted by another transaction");
    if (!(IsToastRelation(relation) || HeapTupleHeaderIsSpeculative(tp.t_data)))
        elog(ERROR, "attempted to kill a non-speculative tuple");

    START_CRIT_SECTION();

    // Set page as prunable
    TransactionId prune_xid = TransactionIdPrecedes(TransactionXmin, relation->rd_rel->relfrozenxid)
                             ? relation->rd_rel->relfrozenxid : TransactionXmin;
    PageSetPrunable(page, prune_xid);

    // Clear infomask bits and make tuple immediately invisible
    tp.t_data->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
    tp.t_data->t_infomask2 &= ~HEAP_KEYS_UPDATED;
    HeapTupleHeaderSetXmin(tp.t_data, InvalidTransactionId);
    tp.t_data->t_ctid = tp.t_self;  // Clear speculative token

    MarkBufferDirty(buffer);

    // WAL logging (same as heap_delete)
    if (RelationNeedsWAL(relation))
    {
        xl_heap_delete xlrec;
        xlrec.flags = XLH_DELETE_IS_SUPER;
        xlrec.infobits_set = compute_infobits(tp.t_data->t_infomask, tp.t_data->t_infomask2);
        xlrec.offnum = ItemPointerGetOffsetNumber(&tp.t_self);
        xlrec.xmax = xid;

        XLogBeginInsert();
        XLogRegisterData((char *) &xlrec, SizeOfHeapDelete);
        XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);
        XLogRecPtr recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_DELETE);
        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

    // Clean up TOAST data if needed
    if (HeapTupleHasExternal(&tp))
        heap_toast_delete(relation, &tp, true);

    ReleaseBuffer(buffer);
    pgstat_count_heap_delete(relation);
}
```