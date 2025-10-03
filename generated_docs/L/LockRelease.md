# LockRelease

## Location
[src/backend/storage/lmgr/lock.c:1964-2168](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L1964-L2168)

## Overview
LockRelease releases one lock of a specified mode on a given lock tag, handling both session and transaction locks while waking up any processes that can now be granted locks.

## Definition

```c
bool
LockRelease(const LOCKTAG *locktag, LOCKMODE lockmode, bool sessionLock)
```
## Detailed Description
LockRelease is the primary function for releasing locks in PostgreSQL's lock management system. It performs a comprehensive release process that includes:

1. **Validation**: Verifies lock method and mode parameters
2. **Local Lock Lookup**: Finds the corresponding LOCALLOCK entry
3. **Owner Management**: Decrements reference counts for the appropriate resource owner
4. **Fast Path Optimization**: Attempts fast-path release for relation locks when possible
5. **Shared Lock Table Operations**: Falls back to shared lock table manipulation if needed
6. **Lock Cleanup**: Calls UnGrantLock and CleanUpLock to update shared state and wake waiters
7. **Local Cleanup**: Removes the LOCALLOCK entry if no longer held

The function handles both regular transaction locks (tied to CurrentResourceOwner) and session locks (not tied to any resource owner). It includes extensive error checking and supports PostgreSQL's fast-path optimization for relation locks.

## Parameters / Member Variables
- `*locktag`: Pointer to LOCKTAG structure identifying the specific lock to release
- `lockmode`: The lock mode being released (e.g., AccessShareLock, ExclusiveLock)
- `sessionLock`: If true, release a session lock; if false, release a transaction lock
## Dependencies
- Functions called/Symbols referenced:
  - [hash_search](../h/hash_search.md)/hash_search_with_hash_value (hash table operations)
  - EligibleForRelationFastPath
  - [FastPathUnGrantRelationLock](../F/FastPathUnGrantRelationLock.md)
  - LockHashPartitionLock
  - [ResourceOwnerForgetLock](../R/ResourceOwnerForgetLock.md)
  - [UnGrantLock](../U/UnGrantLock.md)
  - [CleanUpLock](../C/CleanUpLock.md)
  - [RemoveLocalLock](../R/RemoveLocalLock.md)
  - LOCK_PRINT/PROCLOCK_PRINT (debug macros)
  - LOCKBIT_ON (macro)
  - [LWLockAcquire](LWLockAcquire.md)/LWLockRelease
- Called from (representative examples):
  - [UnlockRelationId](../U/UnlockRelationId.md)
  - [UnlockRelationOid](../U/UnlockRelationOid.md)
  - [UnlockRelation](../U/UnlockRelation.md)
  - [XactLockTableDelete](../X/XactLockTableDelete.md)
  - [SpeculativeInsertionLockRelease](../S/SpeculativeInsertionLockRelease.md)
  - [UnlockDatabaseObject](../U/UnlockDatabaseObject.md)
  - [UnlockSharedObject](../U/UnlockSharedObject.md)
  - pg_advisory_unlock functions

## Notes and Other Information
- Returns true if lock was successfully released, false if not owned by caller
- Supports both fast-path and shared lock table release mechanisms
- Resets lockCleared flag when completely releasing a lock
- May need to re-lookup shared objects if lock was moved from fast-path to shared table
- Includes comprehensive ownership verification to prevent releasing locks owned by other resource owners
- Wakes up waiting processes through CleanUpLock when appropriate
- Located in src/backend/storage/lmgr/lock.c at lines 1964-2168
- Critical for transaction cleanup and lock contention resolution

## Simplified Source

```c
bool LockRelease(const LOCKTAG *locktag, LOCKMODE lockmode, bool sessionLock)
{
    LOCKMETHODID lockmethodid = locktag->locktag_lockmethodid;
    LockMethod lockMethodTable;
    LOCALLOCKTAG localtag;
    LOCALLOCK *locallock;
    LOCK *lock;
    PROCLOCK *proclock;
    LWLock *partitionLock;
    bool wakeupNeeded;

    // Validate lock method and mode
    if (lockmethodid <= 0 || lockmethodid >= lengthof(LockMethods))
        elog(ERROR, "unrecognized lock method: %d", lockmethodid);
    lockMethodTable = LockMethods[lockmethodid];
    if (lockmode <= 0 || lockmode > lockMethodTable->numLockModes)
        elog(ERROR, "unrecognized lock mode: %d", lockmode);

    // Find the LOCALLOCK entry for this lock and lockmode
    MemSet(&localtag, 0, sizeof(localtag));
    localtag.lock = *locktag;
    localtag.mode = lockmode;

    locallock = (LOCALLOCK *) hash_search(LockMethodLocalHash, &localtag, HASH_FIND, NULL);

    // Check if we own the lock
    if (!locallock || locallock->nLocks <= 0) {
        elog(WARNING, "you don't own a lock of type %s", lockMethodTable->lockModeNames[lockmode]);
        return false;
    }

    // Decrease the count for the resource owner
    {
        LOCALLOCKOWNER *lockOwners = locallock->lockOwners;
        ResourceOwner owner = sessionLock ? NULL : CurrentResourceOwner;
        int i;

        for (i = locallock->numLockOwners - 1; i >= 0; i--) {
            if (lockOwners[i].owner == owner) {
                Assert(lockOwners[i].nLocks > 0);
                if (--lockOwners[i].nLocks == 0) {
                    if (owner != NULL)
                        ResourceOwnerForgetLock(owner, locallock);
                    // Compact out unused slot
                    locallock->numLockOwners--;
                    if (i < locallock->numLockOwners)
                        lockOwners[i] = lockOwners[locallock->numLockOwners];
                }
                break;
            }
        }
        if (i < 0) {
            elog(WARNING, "you don't own a lock of type %s", lockMethodTable->lockModeNames[lockmode]);
            return false;
        }
    }

    // Decrease the total local count
    locallock->nLocks--;
    if (locallock->nLocks > 0)
        return true;

    // Reset lock cleared flag
    locallock->lockCleared = false;

    // Try fast path release for relation locks
    if (EligibleForRelationFastPath(locktag, lockmode) && FastPathLocalUseCount > 0) {
        bool released;

        LWLockAcquire(&MyProc->fpInfoLock, LW_EXCLUSIVE);
        released = FastPathUnGrantRelationLock(locktag->locktag_field2, lockmode);
        LWLockRelease(&MyProc->fpInfoLock);
        if (released) {
            RemoveLocalLock(locallock);
            return true;
        }
    }

    // Use shared lock table
    partitionLock = LockHashPartitionLock(locallock->hashcode);
    LWLockAcquire(partitionLock, LW_EXCLUSIVE);

    // Re-find lock and proclock if needed (fast-path -> shared table migration)
    lock = locallock->lock;
    if (!lock) {
        PROCLOCKTAG proclocktag;

        Assert(EligibleForRelationFastPath(locktag, lockmode));
        lock = (LOCK *) hash_search_with_hash_value(LockMethodLockHash, locktag,
                                                   locallock->hashcode, HASH_FIND, NULL);
        if (!lock)
            elog(ERROR, "failed to re-find shared lock object");
        locallock->lock = lock;

        proclocktag.myLock = lock;
        proclocktag.myProc = MyProc;
        locallock->proclock = (PROCLOCK *) hash_search(LockMethodProcLockHash,
                                                      &proclocktag, HASH_FIND, NULL);
        if (!locallock->proclock)
            elog(ERROR, "failed to re-find shared proclock object");
    }

    proclock = locallock->proclock;

    // Double-check that we hold the lock type we want to release
    if (!(proclock->holdMask & LOCKBIT_ON(lockmode))) {
        LWLockRelease(partitionLock);
        elog(WARNING, "you don't own a lock of type %s", lockMethodTable->lockModeNames[lockmode]);
        RemoveLocalLock(locallock);
        return false;
    }

    // Release the lock and wake up waiters
    wakeupNeeded = UnGrantLock(lock, lockmode, proclock, lockMethodTable);
    CleanUpLock(lock, proclock, lockMethodTable, locallock->hashcode, wakeupNeeded);

    LWLockRelease(partitionLock);
    RemoveLocalLock(locallock);
    return true;
}
```