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