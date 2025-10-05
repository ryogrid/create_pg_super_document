# ConditionalLockTuple

## Location
[src/backend/storage/lmgr/lmgr.c:578-594](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L578-L594)

## Overview
Attempt to obtain a tuple-level lock without blocking, returning true if the lock was successfully acquired on the specified tuple.

## Definition
```c
bool ConditionalLockTuple(Relation relation, ItemPointer tid, LOCKMODE lockmode)
```

## Detailed Description
ConditionalLockTuple is a non-blocking variant of LockTuple that attempts to acquire a tuple-level lock on a specific tuple within a relation. Unlike LockTuple, this function will not wait if the lock is not immediately available. It constructs a lock tag using the relations database ID, relation ID, and the tuples block number and offset number extracted from the ItemPointer, then attempts to acquire the lock using the specified lock mode. The function returns true if the lock was successfully acquired, false otherwise. Like LockTuple, this function operates at the most granular locking level in PostgreSQL.

## Parameters / Member Variables
- `relation`: The relation (table) containing the tuple to be locked
- `tid`: ItemPointer that uniquely identifies the tuple (contains block number and offset number)
- `lockmode`: The type of lock to acquire (e.g., AccessShareLock, ExclusiveLock)

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_TUPLE
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
  - [LockAcquire](../L/LockAcquire.md)
  - [LOCKTAG](../L/LOCKTAG.md)
  - LOCKACQUIRE_NOT_AVAIL
- Called from (representative examples):
  - ConditionalLockTupleTuplock
  - [XLTW_Oper](../X/XLTW_Oper.md)

## Notes and Other Information
- This is the non-blocking version of LockTuple that returns immediately rather than waiting for the lock
- Returns true if the lock was acquired, false if it would have required waiting
- Uses the dontWait parameter (true) in LockAcquire to achieve non-blocking behavior
- Checks the return value against LOCKACQUIRE_NOT_AVAIL to determine success/failure
- Operates at the most granular (tuple) level of PostgreSQL locking
- Useful in scenarios where the caller can perform alternative actions if the tuple lock is not immediately available
- Like LockTuple, should be used with careful consideration of the overall locking strategy

## Simplified Source
```c
bool ConditionalLockTuple(Relation relation, ItemPointer tid, LOCKMODE lockmode)
{
    LOCKTAG tag;

    // Create lock tag for the specific tuple
    SET_LOCKTAG_TUPLE(tag,
                      relation->rd_lockInfo.lockRelId.dbId,
                      relation->rd_lockInfo.lockRelId.relId,
                      ItemPointerGetBlockNumber(tid),
                      ItemPointerGetOffsetNumber(tid));

    // Try to acquire lock without waiting - return success/failure
    return (LockAcquire(&tag, lockmode, false, true) != LOCKACQUIRE_NOT_AVAIL);
}
```