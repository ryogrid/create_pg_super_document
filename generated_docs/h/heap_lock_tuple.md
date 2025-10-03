# heap_lock_tuple

## Location
[src/backend/access/heap/heapam.c:4533-5230](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L4533-L5230)

## Overview
heap_lock_tuple is the core function responsible for acquiring shared or exclusive locks on heap tuples, handling complex concurrency control, transaction visibility, and MultiXact management in PostgreSQL.

## Definition

```c
TM_Result
heap_lock_tuple(Relation relation, HeapTuple tuple,
				CommandId cid, LockTupleMode mode, LockWaitPolicy wait_policy,
				bool follow_updates,
				Buffer *buffer, TM_FailureData *tmfd)
```
## Detailed Description
This function implements PostgreSQL's sophisticated tuple locking mechanism, managing concurrent access to individual rows. It handles multiple lock modes (KeyShare, Share, NoKeyExclusive, Exclusive), visibility checking, transaction conflict resolution, and MultiXact management. The function can optionally follow update chains to lock descendant tuples and implements various wait policies.

Key operations include:
1. Buffer management and visibility map optimization
2. Tuple visibility verification using HeapTupleSatisfiesUpdate
3. Conflict detection and resolution with existing lockers/updaters
4. MultiXact and single-transaction lock management
5. Wait policy enforcement (Block, Skip, Error)
6. Update chain following for comprehensive locking
7. Transaction information recording and WAL logging

## Parameters / Member Variables
- `relation`: Relation containing the tuple to lock
- `tuple`: Heap tuple to lock (filled in on output)
- `cid`: Current command ID for visibility testing and storage
- `mode`: Lock mode (KeyShare, Share, NoKeyExclusive, Exclusive)
- `wait_policy`: Behavior when lock unavailable (Block, Skip, Error)
- `follow_updates`: If true, follow update chain to lock descendant tuples
- `*buffer`: Output parameter for buffer containing tuple (pinned but not locked)
- `*tmfd`: Output parameter filled with failure details for non-success cases
## Dependencies
- Functions called/Symbols referenced:
  - [ReadBuffer](../R/ReadBuffer.md), LockBuffer (buffer management)
  - [HeapTupleSatisfiesUpdate](../H/HeapTupleSatisfiesUpdate.md) (visibility checking)
  - [get_mxact_status_for_lock](../g/get_mxact_status_for_lock.md) (MultiXact status mapping)
  - [heap_acquire_tuplock](heap_acquire_tuplock.md) (heavyweight tuple lock acquisition)
  - [MultiXactIdWait](../M/MultiXactIdWait.md), XactLockTableWait (waiting for transactions)
  - [heap_lock_updated_tuple](heap_lock_updated_tuple.md) (follow update chain)
  - [compute_new_xmax_infomask](../c/compute_new_xmax_infomask.md) (compute new transaction information)
  - [visibilitymap_pin](../v/visibilitymap_pin.md), visibilitymap_clear (visibility map management)
  - [UpdateXmaxHintBits](../U/UpdateXmaxHintBits.md) (hint bit updates)
  - [XLogInsert](../X/XLogInsert.md) (WAL logging)
- Type references:
  - TM_Result (tuple manager result codes)
  - [LockTupleMode](../L/LockTupleMode.md) (lock mode enumeration)
  - LockWaitPolicy (wait policy enumeration)
  - [TM_FailureData](../T/TM_FailureData.md) (failure information structure)
  - [MultiXactStatus](../M/MultiXactStatus.md) (MultiXact member status)
- Called from (representative examples):
  - [heapam_tuple_lock](heapam_tuple_lock.md) (heap access method interface)

## Notes and Other Information
- Implements PostgreSQL's sophisticated row-level locking with MultiXact support
- Handles four different lock modes with varying strength and conflict patterns
- Optimizes for common cases to avoid unnecessary waiting
- Manages both lightweight (infomask) and heavyweight (lock manager) tuple locks
- Supports update chain following for comprehensive row locking
- Implements visibility map optimizations for all-visible pages
- Includes comprehensive WAL logging for crash recovery
- Part of PostgreSQL's tuple manager providing MVCC concurrency control
- Critical for implementing SQL-standard isolation levels and FOR SHARE/UPDATE clauses
- [Complex](../C/Complex.md) interaction with MultiXact system for handling multiple concurrent lockers

## Simplified Source

```c
TM_Result
heap_lock_tuple(Relation relation, HeapTuple tuple,
                CommandId cid, LockTupleMode mode, LockWaitPolicy wait_policy,
                bool follow_updates,
                Buffer *buffer, TM_FailureData *tmfd)
{
    TM_Result result;
    ItemPointer tid = &(tuple->t_self);
    ItemId lp;
    Page page;
    Buffer vmbuffer = InvalidBuffer;
    TransactionId xid, xmax;
    uint16 old_infomask, new_infomask, new_infomask2;
    bool first_time = true;
    bool skip_tuple_lock = false;
    bool have_tuple_lock = false;
    bool cleared_all_frozen = false;

    // Read and pin the buffer
    *buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(tid));

    // Pin visibility map if page appears all-visible
    if (PageIsAllVisible(BufferGetPage(*buffer)))
        visibilitymap_pin(relation, ItemPointerGetBlockNumber(tid), &vmbuffer);

    LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);

    // Set up tuple structure
    page = BufferGetPage(*buffer);
    lp = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));
    tuple->t_data = (HeapTupleHeader) PageGetItem(page, lp);
    tuple->t_len = ItemIdGetLength(lp);
    tuple->t_tableOid = RelationGetRelid(relation);

l3:
    // Check tuple visibility and lock status
    result = HeapTupleSatisfiesUpdate(tuple, cid, *buffer);

    if (result == TM_Invisible) {
        result = TM_Invisible;
        goto out_locked;
    }

    // Handle concurrent modifications
    if (result == TM_BeingModified || result == TM_Updated || result == TM_Deleted) {
        TransactionId xwait;
        uint16 infomask, infomask2;
        bool require_sleep;
        ItemPointerData t_ctid;

        // Save transaction state before unlocking
        xwait = HeapTupleHeaderGetRawXmax(tuple->t_data);
        infomask = tuple->t_data->t_infomask;
        infomask2 = tuple->t_data->t_infomask2;
        ItemPointerCopy(&tuple->t_data->t_ctid, &t_ctid);

        LockBuffer(*buffer, BUFFER_LOCK_UNLOCK);

        // Check if we already hold a sufficient lock
        if (first_time) {
            first_time = false;

            if (infomask & HEAP_XMAX_IS_MULTI) {
                // Handle MultiXact case - check for existing strong enough lock
                // Simplified: check if our transaction already has required lock
            } else if (TransactionIdIsCurrentTransactionId(xwait)) {
                // We already hold some lock - check if it's sufficient
                switch (mode) {
                    case LockTupleKeyShare:
                        if (HEAP_XMAX_IS_KEYSHR_LOCKED(infomask) ||
                            HEAP_XMAX_IS_SHR_LOCKED(infomask) ||
                            HEAP_XMAX_IS_EXCL_LOCKED(infomask)) {
                            result = TM_Ok;
                            goto out_unlocked;
                        }
                        break;
                    case LockTupleShare:
                        if (HEAP_XMAX_IS_SHR_LOCKED(infomask) ||
                            HEAP_XMAX_IS_EXCL_LOCKED(infomask)) {
                            result = TM_Ok;
                            goto out_unlocked;
                        }
                        break;
                    // Additional cases for NoKeyExclusive and Exclusive...
                }
            }
        }

        // Determine if we need to sleep/wait
        require_sleep = true;

        // Optimize specific lock modes to avoid unnecessary waiting
        if (mode == LockTupleKeyShare && !(infomask2 & HEAP_KEYS_UPDATED)) {
            // KeyShare lock can often avoid waiting if keys not updated
            if (follow_updates && !HEAP_XMAX_IS_LOCKED_ONLY(infomask)) {
                // Follow update chain to lock descendant tuples
                TM_Result res = heap_lock_updated_tuple(relation, tuple, &t_ctid,
                                                       GetCurrentTransactionId(), mode);
                if (res != TM_Ok) {
                    result = res;
                    LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
                    goto failed;
                }
            }
            require_sleep = false;
        }

        // Wait for conflicting transactions if necessary
        if (require_sleep) {
            if (!skip_tuple_lock &&
                !heap_acquire_tuplock(relation, tid, mode, wait_policy, &have_tuple_lock)) {
                result = TM_WouldBlock;
                LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
                goto failed;
            }

            // Wait for MultiXact or single transaction
            if (infomask & HEAP_XMAX_IS_MULTI) {
                MultiXactStatus status = get_mxact_status_for_lock(mode, false);
                switch (wait_policy) {
                    case LockWaitBlock:
                        MultiXactIdWait((MultiXactId) xwait, status, infomask,
                                       relation, &tuple->t_self, XLTW_Lock, NULL);
                        break;
                    case LockWaitSkip:
                        if (!ConditionalMultiXactIdWait((MultiXactId) xwait, status,
                                                       infomask, relation, NULL)) {
                            result = TM_WouldBlock;
                            LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
                            goto failed;
                        }
                        break;
                    case LockWaitError:
                        // Error if can't acquire immediately
                        break;
                }
            } else {
                // Wait for single transaction
                switch (wait_policy) {
                    case LockWaitBlock:
                        XactLockTableWait(xwait, relation, &tuple->t_self, XLTW_Lock);
                        break;
                    case LockWaitSkip:
                        if (!ConditionalXactLockTableWait(xwait)) {
                            result = TM_WouldBlock;
                            LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
                            goto failed;
                        }
                        break;
                    case LockWaitError:
                        // Error if can't acquire immediately
                        break;
                }
            }

            // Follow update chain if needed
            if (follow_updates && !HEAP_XMAX_IS_LOCKED_ONLY(infomask)) {
                TM_Result res = heap_lock_updated_tuple(relation, tuple, &t_ctid,
                                                       GetCurrentTransactionId(), mode);
                if (res != TM_Ok) {
                    result = res;
                    LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
                    goto failed;
                }
            }
        }

        LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);

        // Recheck tuple state after waiting
        if (xmax_infomask_changed(tuple->t_data->t_infomask, infomask) ||
            !TransactionIdEquals(HeapTupleHeaderGetRawXmax(tuple->t_data), xwait))
            goto l3;

        // Update hint bits for better performance
        if (!(infomask & HEAP_XMAX_IS_MULTI))
            UpdateXmaxHintBits(tuple->t_data, *buffer, xwait);

        // Determine final result
        if (!require_sleep ||
            (tuple->t_data->t_infomask & HEAP_XMAX_INVALID) ||
            HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_data->t_infomask))
            result = TM_Ok;
        else if (!ItemPointerEquals(&tuple->t_self, &tuple->t_data->t_ctid))
            result = TM_Updated;
        else
            result = TM_Deleted;
    }

failed:
    if (result != TM_Ok) {
        // Fill failure information
        tmfd->ctid = tuple->t_data->t_ctid;
        tmfd->xmax = HeapTupleHeaderGetUpdateXid(tuple->t_data);
        if (result == TM_SelfModified)
            tmfd->cmax = HeapTupleHeaderGetCmax(tuple->t_data);
        else
            tmfd->cmax = InvalidCommandId;
        goto out_locked;
    }

    // Pin visibility map if not already done
    if (vmbuffer == InvalidBuffer && PageIsAllVisible(page)) {
        LockBuffer(*buffer, BUFFER_LOCK_UNLOCK);
        visibilitymap_pin(relation, ItemPointerGetBlockNumber(tid), &vmbuffer);
        LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
        goto l3;
    }

    // Prepare to update tuple with lock information
    xmax = HeapTupleHeaderGetRawXmax(tuple->t_data);
    old_infomask = tuple->t_data->t_infomask;

    MultiXactIdSetOldestMember();

    // Compute new transaction info for the tuple
    compute_new_xmax_infomask(xmax, old_infomask, tuple->t_data->t_infomask2,
                             GetCurrentTransactionId(), mode, false,
                             &xid, &new_infomask, &new_infomask2);

    START_CRIT_SECTION();

    // Update tuple with lock information
    tuple->t_data->t_infomask &= ~HEAP_XMAX_BITS;
    tuple->t_data->t_infomask2 &= ~HEAP_KEYS_UPDATED;
    tuple->t_data->t_infomask |= new_infomask;
    tuple->t_data->t_infomask2 |= new_infomask2;
    if (HEAP_XMAX_IS_LOCKED_ONLY(new_infomask))
        HeapTupleHeaderClearHotUpdated(tuple->t_data);
    HeapTupleHeaderSetXmax(tuple->t_data, xid);

    // Update t_ctid for lock-only operations
    if (HEAP_XMAX_IS_LOCKED_ONLY(new_infomask))
        tuple->t_data->t_ctid = *tid;

    // Clear visibility map if needed
    if (PageIsAllVisible(page) &&
        visibilitymap_clear(relation, ItemPointerGetBlockNumber(tid), vmbuffer,
                           VISIBILITYMAP_ALL_FROZEN))
        cleared_all_frozen = true;

    MarkBufferDirty(*buffer);

    // WAL logging for crash recovery
    if (RelationNeedsWAL(relation)) {
        xl_heap_lock xlrec;
        XLogRecPtr recptr;

        XLogBeginInsert();
        XLogRegisterBuffer(0, *buffer, REGBUF_STANDARD);

        xlrec.offnum = ItemPointerGetOffsetNumber(&tuple->t_self);
        xlrec.xmax = xid;
        xlrec.infobits_set = compute_infobits(new_infomask, tuple->t_data->t_infomask2);
        xlrec.flags = cleared_all_frozen ? XLH_LOCK_ALL_FROZEN_CLEARED : 0;
        XLogRegisterData((char *) &xlrec, SizeOfHeapLock);

        recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_LOCK);
        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();

    result = TM_Ok;

out_locked:
    LockBuffer(*buffer, BUFFER_LOCK_UNLOCK);

out_unlocked:
    if (BufferIsValid(vmbuffer))
        ReleaseBuffer(vmbuffer);

    // Release tuple lock if we acquired it
    if (have_tuple_lock)
        UnlockTupleTuplock(relation, tid, mode);

    return result;
}
```