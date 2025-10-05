# LockHasWaiters

## Location
[src/backend/storage/lmgr/lock.c:643-755](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L643-L755)

## Overview
LockHasWaiters determines whether releasing a specific lock would wake up other processes waiting for it, providing essential information for lock release optimization and deadlock detection.

## Definition

```c
bool
LockHasWaiters(const LOCKTAG *locktag, LOCKMODE lockmode, bool sessionLock)
```
## Detailed Description
LockHasWaiters performs a non-intrusive check to determine if other processes are waiting for a lock that the current process holds. It validates that the current process actually owns the specified lock, then examines the shared lock table to check if there are waiters whose lock requests conflict with the specified lock mode. This function is crucial for optimizing lock release operations and understanding contention patterns without actually releasing the lock.

The function operates by first locating the local lock entry in the current backend's hash table, then acquiring a shared lock on the appropriate partition to safely examine the shared lock state. It performs several validation steps to ensure the process actually holds the lock before checking the conflict matrix against waiting processes.

## Parameters / Member Variables
- `*locktag`: Pointer to LOCKTAG structure identifying the specific lock object (table, relation, etc.)
- `lockmode`: The specific lock mode to check for waiters (e.g., AccessExclusiveLock, ShareLock)
- `sessionLock`: Boolean indicating whether this is a session-level lock (currently unused in the implementation)
## Dependencies
- Functions called/Symbols referenced:
  - [hash_search](../h/hash_search.md) (to find LOCALLOCK entry)
  - LockHashPartitionLock (to get partition lock for safe access)
  - [LWLockAcquire](LWLockAcquire.md)/LWLockRelease (for shared lock table synchronization)
  - [RemoveLocalLock](../R/RemoveLocalLock.md) (cleanup on error conditions)
  - LOCKBIT_ON (macro for lock mode bit manipulation)
  - MemSet (memory initialization)
- Data structures used:
  - LOCALTAG, LOCALLOCK, LOCK, PROCLOCK
  - LockMethods array and conflict tables
- Called from (representative examples):
  - [LockHasWaitersRelation](LockHasWaitersRelation.md) (relation-specific wrapper)

## Notes and Other Information
- Returns false and logs warnings if the process doesn't actually own the specified lock
- Uses shared locking on the partition to avoid blocking concurrent lock operations
- The function is read-only and doesn't modify any lock state
- Critical for implementing efficient lock release strategies in high-concurrency scenarios
- Relies on the conflict matrix in LockMethods to determine potential waiter conflicts

## Simplified Source
```c
bool LockHasWaiters(const LOCKTAG *locktag, LOCKMODE lockmode, bool sessionLock)
{
    LOCKMETHODID lockmethodid = locktag->locktag_lockmethodid;
    LockMethod lockMethodTable;
    LOCALLOCKTTAG localtag;
    LOCALLOCK *locallock;
    LOCK *lock;
    PROCLOCK *proclock;
    LWLock *partitionLock;
    bool hasWaiters = false;

    // Validate lock method and mode
    if (lockmethodid <= 0 || lockmethodid >= lengthof(LockMethods))
        elog(ERROR, "unrecognized lock method: %d", lockmethodid);

    lockMethodTable = LockMethods[lockmethodid];
    if (lockmode <= 0 || lockmode > lockMethodTable->numLockModes)
        elog(ERROR, "unrecognized lock mode: %d", lockmode);

    // Find local lock entry
    MemSet(&localtag, 0, sizeof(localtag));
    localtag.lock = *locktag;
    localtag.mode = lockmode;

    locallock = (LOCALLOCK *) hash_search(LockMethodLocalHash, &localtag, HASH_FIND, NULL);

    if (!locallock || locallock->nLocks <= 0)
        return false;

    // Check shared lock table for waiters
    partitionLock = LockHashPartitionLock(locallock->hashcode);
    LWLockAcquire(partitionLock, LW_SHARED);

    lock = locallock->lock;
    proclock = locallock->proclock;

    // Verify we hold the lock
    if (!(proclock->holdMask & LOCKBIT_ON(lockmode))) {
        LWLockRelease(partitionLock);
        return false;
    }

    // Check if releasing this lock would wake waiters
    if ((lockMethodTable->conflictTab[lockmode] & lock->waitMask) != 0)
        hasWaiters = true;

    LWLockRelease(partitionLock);
    return hasWaiters;
}
```