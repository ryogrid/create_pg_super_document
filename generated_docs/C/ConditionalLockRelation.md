# ConditionalLockRelation

## Location
[src/backend/storage/lmgr/lmgr.c:275-309](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L275-L309)

## Overview
ConditionalLockRelation attempts to acquire an additional lock on an already-open relation without blocking, returning immediately if the lock cannot be obtained.

## Definition
```c
bool ConditionalLockRelation(Relation relation, LOCKMODE lockmode)
```

## Detailed Description
This function is the non-blocking variant of LockRelation. It attempts to acquire a lock on an already-open relation using LockAcquireExtended with the dontWait parameter set to true. If the lock cannot be acquired immediately, it returns false without blocking. If successful, it handles invalidation messages similar to LockRelation to maintain cache consistency. This function is useful when lock contention might cause performance issues and the caller can handle the case where the lock is not available.

## Parameters / Member Variables
- `relation`: Pointer to an already-open Relation structure containing the relation information and lock details
- `lockmode`: The lock mode to acquire (e.g., AccessShareLock, RowExclusiveLock, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_RELATION (macro to set up lock tag for relation)
  - [LockAcquireExtended](../L/LockAcquireExtended.md) (performs the actual lock acquisition with extended options)
  - [AcceptInvalidationMessages](../A/AcceptInvalidationMessages.md) (processes cache invalidation messages)
  - [MarkLockClear](../M/MarkLockClear.md) (marks the local lock state as clear)
- Types referenced:
  - [Relation](../R/Relation.md) (relation descriptor structure)
  - [LOCKTAG](../L/LOCKTAG.md) (lock tag structure)
  - [LOCALLOCK](../L/LOCALLOCK.md) (local lock information)
  - LockAcquireResult (result of lock acquisition)
  - LOCKMODE (enumeration of lock modes)
- Called from (representative examples):
  - [lazy_truncate_heap](../l/lazy_truncate_heap.md) (in vacuumlazy.c:2581)

## Notes and Other Information
- Returns true if lock was successfully acquired, false if lock was not available
- This is specifically for adding locks to already-open relations - never use with relation_open(foo, NoLock)
- The function uses dontWait=true in LockAcquireExtended to avoid blocking
- Includes the same cache invalidation handling as LockRelation when the lock is successfully acquired
- Part of PostgreSQL's lock manager subsystem located in src/backend/storage/lmgr/lmgr.c
- Commonly used in vacuum operations where lock contention should be avoided to prevent blocking other operations
- The non-blocking behavior makes it suitable for opportunistic locking scenarios

## Simplified Source

```c
bool ConditionalLockRelation(Relation relation, LOCKMODE lockmode) {
    LOCKTAG tag;
    LOCALLOCK *locallock;
    LockAcquireResult res;

    // Set up lock tag from relation information
    SET_LOCKTAG_RELATION(tag,
                         relation->rd_lockInfo.lockRelId.dbId,
                         relation->rd_lockInfo.lockRelId.relId);

    // Try to acquire lock without waiting
    res = LockAcquireExtended(&tag, lockmode, false, true, true, &locallock);

    if (res == LOCKACQUIRE_NOT_AVAIL)
        return false;

    // Handle cache invalidation if lock was acquired
    if (res != LOCKACQUIRE_ALREADY_CLEAR) {
        AcceptInvalidationMessages();
        MarkLockClear(locallock);
    }

    return true;
}
```