# heap_inplace_lock

## Location
[src/backend/access/heap/heapam.c:6308-6431](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L6308-L6431)

## Overview
Protects inplace updates from concurrent heap_update() operations by evaluating tuple compatibility and acquiring necessary locks.

## Definition
```c
bool heap_inplace_lock(Relation relation, HeapTuple oldtup_ptr, Buffer buffer, void (*release_callback)(void *), void *arg)
```

## Detailed Description
This function determines whether a tuple's state is compatible with a no-key inplace update and protects against concurrent modifications. It evaluates the tuple using HeapTupleSatisfiesUpdate() and handles various tuple states:

1. **Compatible states**: Current transaction rowmarks and KEY SHARE locks from any transaction
2. **Blocking states**: Active updates from other transactions requiring wait
3. **Error states**: Invisible tuples and self-modifications within the same command

The function implements a locking protocol where it first acquires a tuple lock, then a buffer lock. If the tuple state requires waiting for other transactions, it releases locks, calls the provided callback, waits for conflicting transactions, and returns false. Otherwise, it returns true with the buffer still exclusively locked.

Key behavioral aspects:
- Primarily intended for system catalog updates  
- Ensures durability expectations for inplace-updated fields
- Handles MultiXact conflict resolution for complex locking scenarios
- Invalidates catalog snapshots when waiting is required

## Parameters / Member Variables
- `relation`: The heap relation containing the tuple to lock
- `oldtup_ptr`: Pointer to the HeapTuple to be updated inplace
- `buffer`: Buffer containing the tuple data
- `release_callback`: Function to call when locks must be released for waiting
- `arg`: Argument to pass to the release_callback function

## Dependencies
- Functions called/Symbols referenced:
  - [check_inplace_rel_lock](../c/check_inplace_rel_lock.md) (debug builds only)
  - [LockTuple](../L/LockTuple.md)
  - [LockBuffer](../L/LockBuffer.md)  
  - [HeapTupleSatisfiesUpdate](../H/HeapTupleSatisfiesUpdate.md)
  - [GetCurrentCommandId](../G/GetCurrentCommandId.md)
  - HeapTupleHeaderGetRawXmax
  - [DoesMultiXactIdConflict](../D/DoesMultiXactIdConflict.md)
  - [MultiXactIdWait](../M/MultiXactIdWait.md)
  - [TransactionIdIsCurrentTransactionId](../T/TransactionIdIsCurrentTransactionId.md)
  - [XactLockTableWait](../X/XactLockTableWait.md)
  - [UnlockTuple](../U/UnlockTuple.md)
  - [InvalidateCatalogSnapshot](../I/InvalidateCatalogSnapshot.md)
- Called from (representative examples):
  - [systable_inplace_update_begin](../s/systable_inplace_update_begin.md)
  - HeapScanIsValid (indirect reference)

## Notes and Other Information
- Returns true with buffer exclusively locked if update can proceed immediately
- Returns false after waiting if conflicting transactions must complete first
- Caller must release buffer lock via heap_inplace_update_and_unlock(), heap_inplace_unlock(), or error handling
- Designed for system catalogs where SERIALIZABLE isolation doesn't apply to DDL
- Enforces durability expectations for readers of inplace-updated catalog fields
- Uses specialized InplaceUpdateTupleLock for tuple-level locking
- Handles complex MultiXact scenarios for concurrent lock compatibility
- Automatically invalidates catalog snapshots when waiting occurs to ensure subsequent attempts see fresh data

## Simplified Source

```c
// Simplified version of heap_inplace_lock
bool heap_inplace_lock(Relation relation, HeapTuple oldtup_ptr, Buffer buffer,
                       void (*release_callback)(void *), void *arg) {
    HeapTuple oldtup = *oldtup_ptr;
    TM_Result result;
    bool ret;

    // Assert checks and acquire locks
    Assert(BufferIsValid(buffer));
    LockTuple(relation, &oldtup.t_self, InplaceUpdateTupleLock);
    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

    // Check tuple state compatibility with inplace update
    result = HeapTupleSatisfiesUpdate(&oldtup, GetCurrentCommandId(false), buffer);

    // Handle different tuple states
    if (result == TM_Invisible) {
        // Error: Cannot update invisible tuple
        ereport(ERROR, "attempted to overwrite invisible tuple");
    }
    else if (result == TM_SelfModified) {
        // Error: Already modified by current command
        ereport(ERROR, "tuple already modified by current command");
    }
    else if (result == TM_BeingModified) {
        // Tuple is being modified by another transaction
        TransactionId xwait = HeapTupleHeaderGetRawXmax(oldtup.t_data);
        uint16 infomask = oldtup.t_data->t_infomask;

        if (infomask & HEAP_XMAX_IS_MULTI) {
            // Handle MultiXact case - check for conflicts
            if (DoesMultiXactIdConflict(xwait, infomask, LockTupleNoKeyExclusive, NULL)) {
                // Conflict detected - release locks and wait
                LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
                release_callback(arg);
                ret = false;
                MultiXactIdWait(xwait, MultiXactStatusNoKeyUpdate, infomask,
                               relation, &oldtup.t_self, XLTW_Update, NULL);
            } else {
                ret = true;  // No conflict, can proceed
            }
        }
        else if (TransactionIdIsCurrentTransactionId(xwait) ||
                 HEAP_XMAX_IS_KEYSHR_LOCKED(infomask)) {
            // Current transaction or key share lock - compatible
            ret = true;
        }
        else {
            // Other transaction update - release locks and wait
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
            release_callback(arg);
            ret = false;
            XactLockTableWait(xwait, relation, &oldtup.t_self, XLTW_Update);
        }
    }
    else {
        // TM_Ok, TM_Updated, TM_Deleted cases
        ret = (result == TM_Ok);
        if (!ret) {
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
            release_callback(arg);
        }
    }

    // Clean up if we had to wait
    if (!ret) {
        UnlockTuple(relation, &oldtup.t_self, InplaceUpdateTupleLock);
        InvalidateCatalogSnapshot();
    }

    return ret;
}
```

Key simplifications made:
- Removed extensive comments explaining theoretical background
- Simplified MultiXact handling into clear conditional logic
- Consolidated error handling patterns
- Removed debug-only assertions for relation checks
- Streamlined variable declarations and logic flow
- Abstracted complex macro operations with descriptive comments
- Focused on the core algorithm: lock → check compatibility → wait or proceed