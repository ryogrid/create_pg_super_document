# heap_delete

## Location
[src/backend/access/heap/heapam.c:2731-3153](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L2731-L3153)

## Overview
heap_delete is the core function responsible for deleting a tuple from a heap table in PostgreSQL, handling complex visibility rules, transaction concurrency, and logging to ensure ACID compliance.

## Definition

```c
TM_Result
heap_delete(Relation relation, ItemPointer tid,
			CommandId cid, Snapshot crosscheck, bool wait,
			TM_FailureData *tmfd, bool changingPart)
```
## Detailed Description
heap_delete performs the low-level deletion of a heap tuple with comprehensive transaction safety and concurrency control. The function follows PostgreSQL's multi-version concurrency control (MVCC) model, ensuring that concurrent transactions can safely access the same data without blocking each other inappropriately.

The function operates in several phases:
1. **Validation**: Checks for parallel operation restrictions and validates the tuple identifier
2. **Buffer Management**: Reads and locks the target page, managing visibility map interactions
3. **Concurrency Control**: Uses HeapTupleSatisfiesUpdate to check tuple visibility and handles concurrent modifications by waiting for conflicting transactions when necessary
4. **Conflict Resolution**: Manages multi-transaction scenarios and tuple locking to establish deletion priority
5. **Critical Section**: Updates tuple headers with deletion markers, manages visibility information, and logs the operation for crash recovery
6. **Cleanup**: Handles external TOAST data deletion, cache invalidation, and resource cleanup

The function is designed to handle edge cases like tuple updates during deletion attempts, serializable transaction conflicts, and partition moves. It maintains data consistency through careful transaction ID management and proper handling of tuple chains.

## Parameters / Member Variables
- `relation`: The heap relation containing the tuple to delete
- `tid`: ItemPointer identifying the specific tuple location (page and offset)
- `cid`: Command identifier for the current command within the transaction
- `crosscheck`: Optional snapshot for additional visibility validation (used in RI checks)
- `wait`: Boolean indicating whether to wait for concurrent transactions or return immediately
- `*tmfd`: Output structure containing failure details when deletion cannot proceed
- `changingPart`: Boolean flag indicating this deletion is part of a partition move operation
## Dependencies
- Functions called/Symbols referenced:
  - [HeapTupleSatisfiesUpdate](../H/HeapTupleSatisfiesUpdate.md)
  - [GetCurrentTransactionId](../G/GetCurrentTransactionId.md)
  - [CheckForSerializableConflictIn](../C/CheckForSerializableConflictIn.md)
  - [ExtractReplicaIdentity](../E/ExtractReplicaIdentity.md)
  - [compute_new_xmax_infomask](../c/compute_new_xmax_infomask.md)
  - [heap_toast_delete](heap_toast_delete.md)
  - [CacheInvalidateHeapTuple](../C/CacheInvalidateHeapTuple.md)
  - [UpdateXmaxHintBits](../U/UpdateXmaxHintBits.md)
  - [xmax_infomask_changed](../x/xmax_infomask_changed.md)
  - [compute_infobits](../c/compute_infobits.md)
- Called from (representative examples):
  - [simple_heap_delete](../s/simple_heap_delete.md)
  - [heapam_tuple_delete](heapam_tuple_delete.md)

## Notes and Other Information
- The function prohibits execution during parallel operations to prevent combo CID allocation issues
- Implements sophisticated waiting mechanisms for concurrent transactions using tuple-level locking
- Handles both simple transaction waiting and complex multi-transaction conflicts
- Maintains replica identity information for logical replication purposes
- Supports partition movement operations through the changingPart parameter
- Performs extensive validation and assertion checking to ensure data consistency
- Uses critical sections to ensure atomic updates that can be properly recovered from crashes
- The function can return various TM_Result codes indicating success, conflicts, or other conditions requiring caller attention

## Simplified Source

```c
TM_Result heap_delete(Relation relation, ItemPointer tid, CommandId cid,
                     Snapshot crosscheck, bool wait, TM_FailureData *tmfd,
                     bool changingPart) {
    TM_Result result;
    TransactionId xid = GetCurrentTransactionId();
    HeapTupleData tp;
    Page page;
    Buffer buffer;
    Buffer vmbuffer = InvalidBuffer;
    bool have_tuple_lock = false;
    HeapTuple old_key_tuple = NULL;

    // Basic validation
    Assert(ItemPointerIsValid(tid));
    if (IsInParallelMode())
        ereport(ERROR, (errmsg("cannot delete tuples during a parallel operation")));

    // Read the page containing the tuple
    BlockNumber block = ItemPointerGetBlockNumber(tid);
    buffer = ReadBuffer(relation, block);
    page = BufferGetPage(buffer);

    // Pin visibility map if page is all-visible
    if (PageIsAllVisible(page))
        visibilitymap_pin(relation, block, &vmbuffer);

    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

    // Set up tuple structure
    ItemId lp = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));
    tp.t_tableOid = RelationGetRelid(relation);
    tp.t_data = (HeapTupleHeader) PageGetItem(page, lp);
    tp.t_len = ItemIdGetLength(lp);
    tp.t_self = *tid;

retry:
    // Check if we can delete this tuple
    result = HeapTupleSatisfiesUpdate(&tp, cid, buffer);

    if (result == TM_Invisible) {
        UnlockReleaseBuffer(buffer);
        ereport(ERROR, (errmsg("attempted to delete invisible tuple")));
    }

    // Handle concurrent modifications
    if (result == TM_BeingModified && wait) {
        TransactionId xwait = HeapTupleHeaderGetRawXmax(tp.t_data);
        uint16 infomask = tp.t_data->t_infomask;

        // Wait for concurrent transaction
        if (infomask & HEAP_XMAX_IS_MULTI) {
            // Handle multi-transaction case
            MultiXactIdWait((MultiXactId) xwait, MultiXactStatusUpdate, infomask,
                           relation, &(tp.t_self), XLTW_Delete, NULL);
        } else if (!TransactionIdIsCurrentTransactionId(xwait)) {
            // Wait for single transaction
            XactLockTableWait(xwait, relation, &(tp.t_self), XLTW_Delete);
        }

        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
        // Check if conditions changed during wait
        if (xmax_infomask_changed(tp.t_data->t_infomask, infomask))
            goto retry;

        // Update hint bits after waiting
        UpdateXmaxHintBits(tp.t_data, buffer, xwait);
    }

    // Check final visibility after waiting
    if (crosscheck != InvalidSnapshot && result == TM_Ok) {
        if (!HeapTupleSatisfiesVisibility(&tp, crosscheck, buffer))
            result = TM_Updated;
    }

    // Handle deletion failures
    if (result != TM_Ok) {
        tmfd->ctid = tp.t_data->t_ctid;
        tmfd->xmax = HeapTupleHeaderGetUpdateXid(tp.t_data);
        if (result == TM_SelfModified)
            tmfd->cmax = HeapTupleHeaderGetCmax(tp.t_data);
        UnlockReleaseBuffer(buffer);
        return result;
    }

    // Check for serializable conflicts
    CheckForSerializableConflictIn(relation, tid, BufferGetBlockNumber(buffer));

    // Extract replica identity for logical replication
    old_key_tuple = ExtractReplicaIdentity(relation, &tp, true, NULL);

    START_CRIT_SECTION();

    // Mark page as prunable and clear visibility
    PageSetPrunable(page, xid);
    if (PageIsAllVisible(page)) {
        PageClearAllVisible(page);
        visibilitymap_clear(relation, BufferGetBlockNumber(buffer), vmbuffer,
                           VISIBILITYMAP_VALID_BITS);
    }

    // Update tuple header to mark as deleted
    uint16 new_infomask, new_infomask2;
    TransactionId new_xmax;
    compute_new_xmax_infomask(HeapTupleHeaderGetRawXmax(tp.t_data),
                             tp.t_data->t_infomask, tp.t_data->t_infomask2,
                             xid, LockTupleExclusive, true,
                             &new_xmax, &new_infomask, &new_infomask2);

    tp.t_data->t_infomask = new_infomask;
    tp.t_data->t_infomask2 = new_infomask2;
    HeapTupleHeaderSetXmax(tp.t_data, new_xmax);
    HeapTupleHeaderSetCmax(tp.t_data, cid, false);
    tp.t_data->t_ctid = tp.t_self;

    if (changingPart)
        HeapTupleHeaderSetMovedPartitions(tp.t_data);

    MarkBufferDirty(buffer);

    // WAL logging
    if (RelationNeedsWAL(relation)) {
        XLogRecPtr recptr;
        xl_heap_delete xlrec;

        xlrec.offnum = ItemPointerGetOffsetNumber(&tp.t_self);
        xlrec.xmax = new_xmax;
        xlrec.flags = changingPart ? XLH_DELETE_IS_PARTITION_MOVE : 0;

        XLogBeginInsert();
        XLogRegisterData((char *) &xlrec, SizeOfHeapDelete);
        XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

        if (old_key_tuple != NULL) {
            // Log old tuple for logical replication
            xl_heap_header xlhdr;
            xlhdr.t_infomask2 = old_key_tuple->t_data->t_infomask2;
            xlhdr.t_infomask = old_key_tuple->t_data->t_infomask;
            xlhdr.t_hoff = old_key_tuple->t_data->t_hoff;
            XLogRegisterData((char *) &xlhdr, SizeOfHeapHeader);
        }

        recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_DELETE);
        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

    // Clean up TOAST data if present
    if (HeapTupleHasExternal(&tp))
        heap_toast_delete(relation, &tp, false);

    // Invalidate system caches
    CacheInvalidateHeapTuple(relation, &tp, NULL);

    // Resource cleanup
    ReleaseBuffer(buffer);
    if (vmbuffer != InvalidBuffer)
        ReleaseBuffer(vmbuffer);
    if (have_tuple_lock)
        UnlockTupleTuplock(relation, &(tp.t_self), LockTupleExclusive);

    pgstat_count_heap_delete(relation);

    return TM_Ok;
}
```