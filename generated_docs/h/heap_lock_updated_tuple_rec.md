# heap_lock_updated_tuple_rec

## Location
[src/backend/access/heap/heapam.c:5652-5996](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L5652-L5996)

## Overview
heap_lock_updated_tuple_rec recursively locks a tuple and all its updated versions in an update chain, handling complex concurrency scenarios and MultiXactId management.

## Definition
```c
static TM_Result heap_lock_updated_tuple_rec(Relation rel, ItemPointer tid, TransactionId xid,
                                              LockTupleMode mode)
```

## Detailed Description
This static function serves as the recursive core of heap_lock_updated_tuple, implementing the complex logic needed to lock not just a specific tuple version, but all subsequent versions in its update chain. The function operates in a loop that follows tuple update chains by examining each tuple's t_ctid pointer.

For each tuple version encountered, the function:
- Fetches the tuple and validates it exists and wasn't vacuumed
- Handles visibility map pinning for performance optimization
- Checks for transaction conflicts using existing locks (both single TransactionId and MultiXactId)
- Computes new lock information using compute_new_xmax_infomask
- Updates the tuple's lock bits and logs the change via WAL
- Follows the update chain to the next version

The function implements sophisticated conflict detection by examining existing locks and using test_lockmode_for_conflict to determine whether to wait, proceed, or fail. It handles both MultiXactId scenarios (where multiple transactions have locks) and simple TransactionId cases.

A key optimization is detecting when the current transaction already holds a lock on a tuple version (TM_SelfModified), allowing it to skip redundant locking operations.

## Parameters / Member Variables
- `rel`: Relation containing the tuple to be locked
- `tid`: ItemPointer to the starting tuple in the update chain
- `xid`: TransactionId of the transaction requesting the lock
- `mode`: LockTupleMode specifying the strength of lock desired

## Dependencies
- Functions called/Symbols referenced:
  - [heap_fetch](heap_fetch.md)
  - [test_lockmode_for_conflict](../t/test_lockmode_for_conflict.md)
  - [compute_new_xmax_infomask](../c/compute_new_xmax_infomask.md)
  - [GetMultiXactIdMembers](../G/GetMultiXactIdMembers.md)
  - [XactLockTableWait](../X/XactLockTableWait.md)
  - [visibilitymap_pin](../v/visibilitymap_pin.md)
  - [visibilitymap_clear](../v/visibilitymap_clear.md)
  - [compute_infobits](../c/compute_infobits.md)
  - HeapTupleHeaderGetXmin, HeapTupleHeaderGetRawXmax, HeapTupleHeaderSetXmax
  - HeapTupleHeaderGetUpdateXid, HeapTupleHeaderIndicatesMovedPartitions
  - Various WAL logging functions (XLogBeginInsert, XLogRegisterBuffer, etc.)
- Called from (representative examples):
  - [heap_lock_updated_tuple](heap_lock_updated_tuple.md)

## Notes and Other Information
- This is a static function internal to heapam.c, implementing recursive tuple chain locking
- Uses an infinite loop with explicit termination conditions rather than traditional recursion to avoid stack overflow
- Handles complex visibility map management to maintain performance while ensuring consistency
- Implements proper WAL logging for crash recovery with xl_heap_lock_updated records
- The function can restart its processing (goto l4) when it needs to wait for other transactions
- Manages buffer locking carefully to avoid deadlocks while maintaining consistency
- Terminates recursion when reaching the end of update chain (invalid XMAX, moved partitions, or only-locked tuples)
- Critical for maintaining proper tuple locking semantics in PostgreSQL's MVCC system
- Includes extensive error handling for scenarios like aborted transactions and vacuumed tuples
- Performance-optimized with visibility map integration to minimize unnecessary I/O operations

## Simplified Source

```c
static TM_Result
heap_lock_updated_tuple_rec(Relation rel, ItemPointer tid, TransactionId xid,
                            LockTupleMode mode)
{
    TM_Result result;
    ItemPointerData tupid;
    HeapTupleData mytup;
    Buffer buf;
    uint16 new_infomask, new_infomask2, old_infomask, old_infomask2;
    TransactionId xmax, new_xmax;
    TransactionId priorXmax = InvalidTransactionId;
    bool cleared_all_frozen = false;
    bool pinned_desired_page;
    Buffer vmbuffer = InvalidBuffer;
    BlockNumber block;

    ItemPointerCopy(tid, &tupid);

    // Loop through the update chain, locking each version
    for (;;) {
        new_infomask = 0;
        new_xmax = InvalidTransactionId;
        block = ItemPointerGetBlockNumber(&tupid);
        ItemPointerCopy(&tupid, &(mytup.t_self));

        // Fetch the current tuple version
        if (!heap_fetch(rel, SnapshotAny, &mytup, &buf, false)) {
            // Tuple was vacuumed/pruned - chain ends here
            result = TM_Ok;
            goto out_unlocked;
        }

l4:
        CHECK_FOR_INTERRUPTS();

        // Pin visibility map if page appears all-visible
        if (PageIsAllVisible(BufferGetPage(buf))) {
            visibilitymap_pin(rel, block, &vmbuffer);
            pinned_desired_page = true;
        } else {
            pinned_desired_page = false;
        }

        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

        // Recheck visibility map after locking
        if (!pinned_desired_page && PageIsAllVisible(BufferGetPage(buf))) {
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
            visibilitymap_pin(rel, block, &vmbuffer);
            LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        }

        // Validate chain integrity
        if (TransactionIdIsValid(priorXmax) &&
            !TransactionIdEquals(HeapTupleHeaderGetXmin(mytup.t_data), priorXmax)) {
            result = TM_Ok;  // Chain broken
            goto out_locked;
        }

        // Check if tuple was created by aborted transaction
        if (TransactionIdDidAbort(HeapTupleHeaderGetXmin(mytup.t_data))) {
            result = TM_Ok;  // Previous live tuple already locked
            goto out_locked;
        }

        old_infomask = mytup.t_data->t_infomask;
        old_infomask2 = mytup.t_data->t_infomask2;
        xmax = HeapTupleHeaderGetRawXmax(mytup.t_data);

        // Handle existing locks/updates on this tuple version
        if (!(old_infomask & HEAP_XMAX_INVALID)) {
            TransactionId rawxmax = HeapTupleHeaderGetRawXmax(mytup.t_data);
            bool needwait;

            if (old_infomask & HEAP_XMAX_IS_MULTI) {
                // Handle MultiXactId case
                int nmembers, i;
                MultiXactMember *members;

                nmembers = GetMultiXactIdMembers(rawxmax, &members, false,
                                               HEAP_XMAX_IS_LOCKED_ONLY(old_infomask));

                for (i = 0; i < nmembers; i++) {
                    result = test_lockmode_for_conflict(members[i].status,
                                                       members[i].xid, mode,
                                                       &mytup, &needwait);

                    if (result == TM_SelfModified) {
                        // Already locked by us - skip this version
                        pfree(members);
                        goto next;
                    }

                    if (needwait) {
                        // Wait for conflicting transaction
                        LockBuffer(buf, BUFFER_LOCK_UNLOCK);
                        XactLockTableWait(members[i].xid, rel, &mytup.t_self,
                                         XLTW_LockUpdated);
                        pfree(members);
                        goto l4;  // Restart after waiting
                    }

                    if (result != TM_Ok) {
                        pfree(members);
                        goto out_locked;
                    }
                }
                if (members)
                    pfree(members);
            } else {
                // Handle single TransactionId case
                MultiXactStatus status;

                // Convert infomask bits to MultiXactStatus
                if (HEAP_XMAX_IS_LOCKED_ONLY(old_infomask)) {
                    if (HEAP_XMAX_IS_KEYSHR_LOCKED(old_infomask))
                        status = MultiXactStatusForKeyShare;
                    else if (HEAP_XMAX_IS_SHR_LOCKED(old_infomask))
                        status = MultiXactStatusForShare;
                    else if (HEAP_XMAX_IS_EXCL_LOCKED(old_infomask)) {
                        status = (old_infomask2 & HEAP_KEYS_UPDATED) ?
                                MultiXactStatusForUpdate : MultiXactStatusForNoKeyUpdate;
                    } else {
                        elog(ERROR, "invalid lock status in tuple");
                    }
                } else {
                    // It's an update
                    status = (old_infomask2 & HEAP_KEYS_UPDATED) ?
                            MultiXactStatusUpdate : MultiXactStatusNoKeyUpdate;
                }

                result = test_lockmode_for_conflict(status, rawxmax, mode,
                                                   &mytup, &needwait);

                if (result == TM_SelfModified)
                    goto next;  // Already locked by us

                if (needwait) {
                    LockBuffer(buf, BUFFER_LOCK_UNLOCK);
                    XactLockTableWait(rawxmax, rel, &mytup.t_self,
                                     XLTW_LockUpdated);
                    goto l4;  // Restart after waiting
                }

                if (result != TM_Ok)
                    goto out_locked;
            }
        }

        // Compute new lock information for this tuple
        compute_new_xmax_infomask(xmax, old_infomask, mytup.t_data->t_infomask2,
                                 xid, mode, false,
                                 &new_xmax, &new_infomask, &new_infomask2);

        // Clear visibility map if needed
        if (PageIsAllVisible(BufferGetPage(buf)) &&
            visibilitymap_clear(rel, block, vmbuffer, VISIBILITYMAP_ALL_FROZEN))
            cleared_all_frozen = true;

        START_CRIT_SECTION();

        // Update tuple with new lock information
        HeapTupleHeaderSetXmax(mytup.t_data, new_xmax);
        mytup.t_data->t_infomask &= ~HEAP_XMAX_BITS;
        mytup.t_data->t_infomask2 &= ~HEAP_KEYS_UPDATED;
        mytup.t_data->t_infomask |= new_infomask;
        mytup.t_data->t_infomask2 |= new_infomask2;

        MarkBufferDirty(buf);

        // WAL logging for crash recovery
        if (RelationNeedsWAL(rel)) {
            xl_heap_lock_updated xlrec;
            XLogRecPtr recptr;

            XLogBeginInsert();
            XLogRegisterBuffer(0, buf, REGBUF_STANDARD);

            xlrec.offnum = ItemPointerGetOffsetNumber(&mytup.t_self);
            xlrec.xmax = new_xmax;
            xlrec.infobits_set = compute_infobits(new_infomask, new_infomask2);
            xlrec.flags = cleared_all_frozen ? XLH_LOCK_ALL_FROZEN_CLEARED : 0;

            XLogRegisterData((char *) &xlrec, SizeOfHeapLockUpdated);
            recptr = XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_LOCK_UPDATED);
            PageSetLSN(BufferGetPage(buf), recptr);
        }

        END_CRIT_SECTION();

next:
        // Check if we've reached the end of the update chain
        if (mytup.t_data->t_infomask & HEAP_XMAX_INVALID ||
            HeapTupleHeaderIndicatesMovedPartitions(mytup.t_data) ||
            ItemPointerEquals(&mytup.t_self, &mytup.t_data->t_ctid) ||
            HeapTupleHeaderIsOnlyLocked(mytup.t_data)) {
            result = TM_Ok;
            goto out_locked;
        }

        // Move to next tuple in the update chain
        priorXmax = HeapTupleHeaderGetUpdateXid(mytup.t_data);
        ItemPointerCopy(&(mytup.t_data->t_ctid), &tupid);
        UnlockReleaseBuffer(buf);
    }

    result = TM_Ok;

out_locked:
    UnlockReleaseBuffer(buf);

out_unlocked:
    if (vmbuffer != InvalidBuffer)
        ReleaseBuffer(vmbuffer);

    return result;
}
```