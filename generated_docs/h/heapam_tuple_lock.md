# heapam_tuple_lock

## Location
src/backend/access/heap/heapam_handler.c: 360 - 580

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
  - heap_lock_tuple
  - heap_fetch
  - ExecStorePinnedBufferHeapTuple
  - ItemPointerEquals, ItemPointerIndicatesMovedPartitions
  - HeapTupleHeaderIsSpeculative, HeapTupleHeaderGetXmin, HeapTupleHeaderGetCmin, HeapTupleHeaderGetUpdateXid
  - TransactionIdEquals, TransactionIdIsCurrentTransactionId
  - XactLockTableWait, ConditionalXactLockTableWait
  - InitDirtySnapshot, ReleaseBuffer
- Types and constants:
  - TM_Result, TM_FailureData, BufferHeapTupleTableSlot
  - LockTupleMode, LockWaitPolicy, CommandId
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