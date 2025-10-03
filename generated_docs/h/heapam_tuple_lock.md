# heapam_tuple_lock

## Location
[src/backend/access/heap/heapam_handler.c:360-580](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L360-L580)

## Overview
Implements the heap table access method interface for acquiring locks on tuples, with sophisticated logic for following update chains and handling various lock wait policies.

## Definition
```c
static TM_Result heapam_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot, CommandId cid, LockTupleMode mode, LockWaitPolicy wait_policy, uint8 flags, TM_FailureData *tmfd)
```

## Detailed Description
This function provides the heap-specific implementation of tuple locking within PostgreSQL's table access method framework. It handles the complex scenarios that can arise during tuple locking, particularly when dealing with updated tuples and update chains.

The function supports two primary operating modes controlled by flags:
1. **Basic locking**: Attempts to lock the specified tuple using `heap_lock_tuple`
2. **Update chain following**: When `TUPLE_LOCK_FLAG_FIND_LAST_VERSION` is set and the tuple has been updated, follows the update chain to find and lock the latest version

The update chain following logic is particularly sophisticated, handling:
- Partition movement detection and appropriate error reporting
- Transaction visibility checks to ensure tuple consistency
- Proper handling of tuples modified by the current transaction
- Different wait policies (block, skip, error) when encountering locked tuples
- Detection of deleted tuples in the update chain

The function uses a dirty snapshot to traverse update chains, ensuring it can see all versions of the tuple regardless of transaction visibility rules. It includes extensive validation to prevent locking inconsistent or stale tuple versions.

## Parameters / Member Variables
- `relation`: The heap relation containing the tuple to lock
- `tid`: ItemPointer identifying the tuple to lock (may be updated during chain following)
- `snapshot`: Snapshot for visibility checks (not heavily used in current implementation)
- `slot`: BufferHeapTupleTableSlot to store the locked tuple
- `cid`: CommandId for visibility and concurrency control
- `mode`: LockTupleMode specifying the type of lock to acquire
- `wait_policy`: LockWaitPolicy (LockWaitBlock, LockWaitSkip, LockWaitError) for handling lock conflicts
- `flags`: Control flags including TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS and TUPLE_LOCK_FLAG_FIND_LAST_VERSION
- `tmfd`: TM_FailureData structure to receive detailed information about lock attempts

## Dependencies
- Functions called/Symbols referenced:
  - [heap_lock_tuple](heap_lock_tuple.md)
  - [heap_fetch](heap_fetch.md)
  - [ExecStorePinnedBufferHeapTuple](../E/ExecStorePinnedBufferHeapTuple.md)
  - [ItemPointerEquals](../I/ItemPointerEquals.md), ItemPointerIndicatesMovedPartitions
  - HeapTupleHeaderIsSpeculative, HeapTupleHeaderGetXmin, HeapTupleHeaderGetCmin, HeapTupleHeaderGetUpdateXid
  - TransactionIdEquals, TransactionIdIsCurrentTransactionId
  - [XactLockTableWait](../X/XactLockTableWait.md), ConditionalXactLockTableWait
  - InitDirtySnapshot, ReleaseBuffer
- Types and constants:
  - TM_Result, TM_FailureData, BufferHeapTupleTableSlot
  - [LockTupleMode](../L/LockTupleMode.md), LockWaitPolicy, CommandId
  - Various TM_* result codes and TUPLE_LOCK_FLAG_* constants
- Called from (representative examples):
  - Used through table access method interface (no direct callers found in indexed code)

## Notes and Other Information
- This is a static function within heapam_handler.c, part of the heap table access method implementation
- The function can follow arbitrarily long update chains when TUPLE_LOCK_FLAG_FIND_LAST_VERSION is set
- Includes sophisticated error handling for moved partitions and transaction consistency violations
- Uses a retry mechanism (goto tuple_lock_retry) when following update chains leads to finding a live tuple
- The function assumes the slot is a BufferHeapTupleTableSlot and includes assertions to verify this
- Handles memory management by transferring buffer pins to the slot appropriately
- Part of PostgreSQL's pluggable table access method architecture
- The traversed flag in tmfd indicates whether the function had to follow an update chain
- Supports different lock modes (shared, exclusive) and wait policies for maximum flexibility

## Simplified Source

```c
static TM_Result
heapam_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot,
                  TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
                  LockWaitPolicy wait_policy, uint8 flags,
                  TM_FailureData *tmfd)
{
    BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;
    TM_Result result;
    Buffer buffer;
    HeapTuple tuple = &bslot->base.tupdata;
    bool follow_updates = (flags & TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS) != 0;

    tmfd->traversed = false;
    Assert(TTS_IS_BUFFERTUPLE(slot));

tuple_lock_retry:
    // Attempt to lock the tuple
    tuple->t_self = *tid;
    result = heap_lock_tuple(relation, tuple, cid, mode, wait_policy,
                             follow_updates, &buffer, tmfd);

    // If tuple was updated and we need to find the latest version
    if (result == TM_Updated && (flags & TUPLE_LOCK_FLAG_FIND_LAST_VERSION)) {

        ReleaseBuffer(buffer);

        if (!ItemPointerEquals(&tmfd->ctid, &tuple->t_self)) {
            // Follow the update chain to find the latest version
            *tid = tmfd->ctid;
            TransactionId priorXmax = tmfd->xmax;
            tmfd->traversed = true;

            // Search for the latest version in the update chain
            SnapshotData SnapshotDirty;
            InitDirtySnapshot(SnapshotDirty);

            for (;;) {
                // Handle partition movement
                if (ItemPointerIndicatesMovedPartitions(tid))
                    ereport(ERROR, /*... partition movement error ...*/);

                tuple->t_self = *tid;
                if (heap_fetch(relation, &SnapshotDirty, tuple, &buffer, true)) {
                    // Validate tuple consistency
                    if (!TransactionIdEquals(HeapTupleHeaderGetXmin(tuple->t_data), priorXmax)) {
                        ReleaseBuffer(buffer);
                        return TM_Deleted;
                    }

                    // Handle concurrent updates with appropriate wait policy
                    if (TransactionIdIsValid(SnapshotDirty.xmax)) {
                        ReleaseBuffer(buffer);
                        switch (wait_policy) {
                            case LockWaitBlock:
                                XactLockTableWait(SnapshotDirty.xmax, relation, &tuple->t_self, XLTW_FetchUpdated);
                                break;
                            case LockWaitSkip:
                                if (!ConditionalXactLockTableWait(SnapshotDirty.xmax))
                                    return TM_WouldBlock;
                                break;
                            case LockWaitError:
                                if (!ConditionalXactLockTableWait(SnapshotDirty.xmax))
                                    ereport(ERROR, /*... lock not available error ...*/);
                                break;
                        }
                        continue;
                    }

                    // Check for self-modification
                    if (TransactionIdIsCurrentTransactionId(priorXmax) &&
                        HeapTupleHeaderGetCmin(tuple->t_data) >= cid) {
                        tmfd->xmax = priorXmax;
                        tmfd->cmax = HeapTupleHeaderGetCmin(tuple->t_data);
                        ReleaseBuffer(buffer);
                        return TM_SelfModified;
                    }

                    // Found live tuple, retry locking
                    ReleaseBuffer(buffer);
                    goto tuple_lock_retry;
                }

                // Handle empty slot or deleted tuple
                if (tuple->t_data == NULL) {
                    return TM_Deleted;
                }

                // Continue following update chain if tuple was updated
                if (ItemPointerEquals(&tuple->t_self, &tuple->t_data->t_ctid)) {
                    ReleaseBuffer(buffer);
                    return TM_Deleted;
                }

                *tid = tuple->t_data->t_ctid;
                priorXmax = HeapTupleHeaderGetUpdateXid(tuple->t_data);
                ReleaseBuffer(buffer);
            }
        } else {
            return TM_Deleted;
        }
    }

    // Set table OID and store tuple in slot
    slot->tts_tableOid = RelationGetRelid(relation);
    tuple->t_tableOid = slot->tts_tableOid;
    ExecStorePinnedBufferHeapTuple(tuple, slot, buffer);

    return result;
}
```