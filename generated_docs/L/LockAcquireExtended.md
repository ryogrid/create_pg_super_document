# LockAcquireExtended

## Location
[src/backend/storage/lmgr/lock.c:780-1182](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L780-L1182)

## Overview
LockAcquireExtended is the core lock acquisition implementation in PostgreSQL that handles all aspects of acquiring locks including fast-path optimization, conflict detection, waiting, and error handling with extended configuration options.

## Definition

```c
LockAcquireResult
LockAcquireExtended(const LOCKTAG *locktag,
					LOCKMODE lockmode,
					bool sessionLock,
					bool dontWait,
					bool reportMemoryError,
					LOCALLOCK **locallockp)
```
## Detailed Description
LockAcquireExtended is the comprehensive lock acquisition function that implements PostgreSQL's sophisticated locking mechanism. It handles the complete lifecycle of lock acquisition including validation, fast-path optimization for relation locks, conflict detection, shared memory management, and integration with WAL logging for standby servers.

The function employs several optimization strategies: it first checks for existing local locks to avoid shared memory operations, attempts fast-path acquisition for eligible relation locks, and only falls back to the full shared lock table when necessary. It manages memory allocation for lock ownership tracking and handles various error conditions gracefully.

For locks that conflict with the fast-path mechanism, it transfers existing fast-path locks to the shared table. The function integrates with PostgreSQL's deadlock detection system and can optionally wait for lock availability or return immediately based on the dontWait parameter.

## Parameters / Member Variables
- : Pointer to LOCKTAG structure uniquely identifying the lockable object
- : The specific lock mode to acquire (ShareLock, ExclusiveLock, etc.)
- : If true, acquire lock for session rather than current transaction
- : If true, return immediately if lock cannot be acquired without waiting
- : If true, generate ERROR on memory exhaustion; if false, return LOCKACQUIRE_NOT_AVAIL
- : Optional output parameter to receive pointer to LOCALLOCK entry

## Dependencies
- Functions called/Symbols referenced:
  - [SetupLockInTable](../S/SetupLockInTable.md) (creates/finds shared lock and proclock entries)
  - [LockCheckConflicts](LockCheckConflicts.md) (checks for conflicts with existing locks)
  - [GrantLock](../G/GrantLock.md), GrantLockLocal (grants locks in shared and local tables)
  - [WaitOnLock](../W/WaitOnLock.md) (handles waiting for lock availability)
  - [FastPathGrantRelationLock](../F/FastPathGrantRelationLock.md) (attempts fast-path acquisition)
  - [FastPathTransferRelationLocks](../F/FastPathTransferRelationLocks.md) (migrates fast-path locks to shared table)
  - [LogAccessExclusiveLock](LogAccessExclusiveLock.md) (WAL logging for standby replay)
  - [RecoveryInProgress](../R/RecoveryInProgress.md) (recovery state checking)
- Data structures used:
  - [LOCALLOCK](LOCALLOCK.md), LOCK, PROCLOCK (core lock structures)
  - [LOCALLOCKOWNER](LOCALLOCKOWNER.md) (ownership tracking)
  - LockMethods array (lock method configuration)
- Called from (representative examples):
  - [LockAcquire](LockAcquire.md) (public API wrapper)
  - [LockRelationOid](LockRelationOid.md), LockRelation (relation locking)
  - [ConditionalLockRelation](../C/ConditionalLockRelation.md) (non-blocking relation locks)

## Notes and Other Information
- Returns LOCKACQUIRE_OK on success, LOCKACQUIRE_ALREADY_HELD for existing locks, LOCKACQUIRE_NOT_AVAIL when unavailable
- Implements fast-path optimization for relation locks to reduce contention on shared lock table
- Integrates with WAL logging to ensure lock acquisition can be replayed on standby servers
- Handles memory allocation failures gracefully when reportMemoryError is false
- Enforces recovery-time restrictions preventing strong locks during database recovery
- Uses partition locking to reduce contention on the shared lock hash table
- Critical for maintaining ACID properties and preventing race conditions in concurrent access

## Simplified Source

```c
// Simplified version of LockAcquireExtended
LockAcquireResult
LockAcquireExtended(const LOCKTAG *locktag,
                    LOCKMODE lockmode,
                    bool sessionLock,
                    bool dontWait,
                    bool reportMemoryError,
                    LOCALLOCK **locallockp)
{
    LOCKMETHOD lockMethodTable;
    LOCALLOCK *locallock;
    LOCK *lock;
    PROCLOCK *proclock;
    ResourceOwner owner;
    uint32 hashcode;
    LWLock *partitionLock;
    bool found_conflict;
    bool log_lock = false;

    // Validate lock method and mode
    lockMethodTable = LockMethods[locktag->locktag_lockmethodid];
    if (lockmode <= 0 || lockmode > lockMethodTable->numLockModes)
        elog(ERROR, "unrecognized lock mode: %d", lockmode);

    // Check recovery restrictions for strong locks
    if (RecoveryInProgress() && !InRecovery && lockmode > RowExclusiveLock)
        ereport(ERROR, "cannot acquire strong locks during recovery");

    // Determine lock owner
    owner = sessionLock ? NULL : CurrentResourceOwner;

    // Find or create local lock entry
    LOCALLOCKTAG localtag;
    localtag.lock = *locktag;
    localtag.mode = lockmode;

    locallock = hash_search(LockMethodLocalHash, &localtag, HASH_ENTER, &found);
    if (!found) {
        // Initialize new local lock
        locallock->lock = NULL;
        locallock->proclock = NULL;
        locallock->nLocks = 0;
        locallock->numLockOwners = 0;
        locallock->maxLockOwners = 8;
        locallock->lockOwners = MemoryContextAlloc(TopMemoryContext,
                                                   8 * sizeof(LOCALLOCKOWNER));
    }

    hashcode = locallock->hashcode;
    if (locallockp)
        *locallockp = locallock;

    // If we already hold this lock, just increment count
    if (locallock->nLocks > 0) {
        GrantLockLocal(locallock, owner);
        return locallock->lockCleared ? LOCKACQUIRE_ALREADY_CLEAR : LOCKACQUIRE_ALREADY_HELD;
    }

    // Prepare WAL logging for AccessExclusive relation locks
    if (lockmode >= AccessExclusiveLock &&
        locktag->locktag_type == LOCKTAG_RELATION &&
        !RecoveryInProgress() && XLogStandbyInfoActive()) {
        LogAccessExclusiveLockPrepare();
        log_lock = true;
    }

    // Try fast-path acquisition for eligible relation locks
    if (EligibleForRelationFastPath(locktag, lockmode) &&
        FastPathLocalUseCount < FP_LOCK_SLOTS_PER_BACKEND) {

        LWLockAcquire(&MyProc->fpInfoLock, LW_EXCLUSIVE);
        bool acquired = FastPathGrantRelationLock(locktag->locktag_field2, lockmode);
        LWLockRelease(&MyProc->fpInfoLock);

        if (acquired) {
            locallock->lock = NULL;
            locallock->proclock = NULL;
            GrantLockLocal(locallock, owner);
            return LOCKACQUIRE_OK;
        }
    }

    // Handle conflicts with fast-path locks
    if (ConflictsWithRelationFastPath(locktag, lockmode)) {
        if (!FastPathTransferRelationLocks(lockMethodTable, locktag, hashcode)) {
            return reportMemoryError ?
                   ereport(ERROR, "out of shared memory") : LOCKACQUIRE_NOT_AVAIL;
        }
    }

    // Acquire partition lock and setup shared lock table entries
    partitionLock = LockHashPartitionLock(hashcode);
    LWLockAcquire(partitionLock, LW_EXCLUSIVE);

    proclock = SetupLockInTable(lockMethodTable, MyProc, locktag, hashcode, lockmode);
    if (!proclock) {
        LWLockRelease(partitionLock);
        return reportMemoryError ?
               ereport(ERROR, "out of shared memory") : LOCKACQUIRE_NOT_AVAIL;
    }

    locallock->proclock = proclock;
    lock = proclock->tag.myLock;
    locallock->lock = lock;

    // Check for conflicts
    if (lockMethodTable->conflictTab[lockmode] & lock->waitMask)
        found_conflict = true;
    else
        found_conflict = LockCheckConflicts(lockMethodTable, lockmode, lock, proclock);

    if (!found_conflict) {
        // No conflict - grant lock immediately
        GrantLock(lock, proclock, lockmode);
        GrantLockLocal(locallock, owner);
    } else {
        // Conflict exists - wait for lock or return if dontWait
        WaitOnLock(locallock, owner, dontWait);

        if (!(proclock->holdMask & LOCKBIT_ON(lockmode))) {
            if (dontWait) {
                // Clean up and return not available
                LWLockRelease(partitionLock);
                return LOCKACQUIRE_NOT_AVAIL;
            } else {
                elog(ERROR, "LockAcquire failed");
            }
        }
    }

    LWLockRelease(partitionLock);

    // Log the lock acquisition if needed
    if (log_lock) {
        LogAccessExclusiveLock(locktag->locktag_field1, locktag->locktag_field2);
    }

    return LOCKACQUIRE_OK;
}
```

Key simplifications made:
- Removed extensive debugging and error handling details
- Simplified fast-path logic while preserving core functionality
- Consolidated memory management and error recovery
- Preserved all major code paths (fast-path, shared table, waiting)
- Maintained essential conflict detection and resolution logic
- Focused on the main algorithmic flow rather than edge cases