# LockReleaseAll

## Location
[src/backend/storage/lmgr/lock.c:2169-2443](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L2169-L2443)

## Overview
LockReleaseAll releases all locks of a specified lock method held by the current process, with options to release either all locks including session locks or only non-session locks.

## Definition

```c
structures, we must acquire it before attempting
			 * to release the lock via the fast-path.  We will continue to
			 * hold the LWLock until we're done scanning the locallock table,
			 * unless we hit a transferred fast-path lock.  (XXX is this
			 * really such a good idea?  There could be a lot of entries ...)
			 */
			if (!have_fast_path_lwlock)
			{
				LWLockAcquire(&MyProc->fpInfoLock, LW_EXCLUSIVE);
				have_fast_path_lwlock = true;
			}

			/* Attempt fast-path release. */
			relid = locallock->tag.lock.locktag_field2;
```
## Detailed Description
LockReleaseAll is a comprehensive lock cleanup function that releases multiple locks held by the current process for a specific lock method. The function operates in two main phases:

1. **Local Lock Table Scan**: Iterates through the process's local lock table (LOCALLOCK entries), handling:
   - Fast-path lock releases for relation locks
   - Session vs. transaction lock differentiation
   - Resource owner cleanup
   - Marking locks for release in the shared lock table

2. **Shared Lock Table Scan**: Processes each lock partition to:
   - Release locks marked in the releaseMask
   - Handle locks that may have been missed in the local table scan
   - Wake up waiting processes through CleanUpLock

Key features:
- Supports both complete cleanup (allLocks=true) and selective cleanup (allLocks=false)
- Handles PostgreSQL's fast-path optimization for relation locks
- Includes special handling for virtual transaction locks
- Performs extensive validation and debugging checks
- Manages resource owner relationships properly

## Parameters / Member Variables
- `LW_EXCLUSIVE)`: Identifier of the lock method whose locks should be released (e.g., DEFAULT_LOCKMETHOD)
- `true`: If true, release all locks including session locks; if false, release only non-session (transaction) locks

## Dependencies
- Functions called/Symbols referenced:
  - [VirtualXactLockTableCleanup](../V/VirtualXactLockTableCleanup.md)
  - [hash_seq_init](../h/hash_seq_init.md)/hash_seq_search (hash table iteration)
  - [RemoveLocalLock](../R/RemoveLocalLock.md)
  - LOCALLOCK_LOCKMETHOD/LOCALLOCK_LOCKTAG (macros)
  - [ResourceOwnerForgetLock](../R/ResourceOwnerForgetLock.md)
  - EligibleForRelationFastPath
  - [FastPathUnGrantRelationLock](../F/FastPathUnGrantRelationLock.md)
  - [LockRefindAndRelease](LockRefindAndRelease.md)
  - LockHashPartitionLockByIndex
  - [UnGrantLock](../U/UnGrantLock.md)
  - [CleanUpLock](../C/CleanUpLock.md)
  - [LockTagHashCode](LockTagHashCode.md)
  - dlist operations (dlist_foreach_modify, dlist_container, etc.)
- Called from (representative examples):
  - [DiscardAll](../D/DiscardAll.md)
  - [logicalrep_worker_onexit](../l/logicalrep_worker_onexit.md)
  - [ProcReleaseLocks](../P/ProcReleaseLocks.md)
  - [ShutdownPostgres](../S/ShutdownPostgres.md)
  - LockHashPartitionLockByProc

## Notes and Other Information
- Two-phase approach prevents dangling pointers between local and shared lock tables
- Special handling for VXID (Virtual Transaction ID) locks via VirtualXactLockTableCleanup
- Includes debugging warnings for tuple locks held at commit (should be short-duration)
- Fast-path locks require special handling and may need to be "refound" in shared table
- Uses partition-based locking to minimize contention during cleanup
- Extensive assertions verify lock state consistency throughout the process
- Located in src/backend/storage/lmgr/lock.c at lines 2169-2443
- Critical for transaction abort/commit cleanup and session termination
- Optimizes empty partition scanning to avoid unnecessary lock acquisition

## Simplified Source

```c
// Simplified version of LockReleaseAll
void LockReleaseAll(LOCKMETHODID lockmethodid, bool allLocks)
{
    LOCALLOCK  *locallock;
    LOCK       *lock;
    int         partition;
    bool        have_fast_path_lwlock = false;

    // Validate lock method ID
    if (lockmethodid <= 0 || lockmethodid >= lengthof(LockMethods))
        elog(ERROR, "unrecognized lock method: %d", lockmethodid);

    LockMethod lockMethodTable = LockMethods[lockmethodid];

    // Special cleanup for virtual transaction locks
    if (lockmethodid == DEFAULT_LOCKMETHOD)
        VirtualXactLockTableCleanup();

    // Phase 1: Process local lock table entries
    hash_seq_init(&status, LockMethodLocalHash);
    while ((locallock = hash_seq_search(&status)) != NULL)
    {
        // Skip unused or wrong lock method entries
        if (locallock->nLocks == 0 ||
            LOCALLOCK_LOCKMETHOD(*locallock) != lockmethodid) {
            if (locallock->nLocks == 0)
                RemoveLocalLock(locallock);
            continue;
        }

        // Handle session vs transaction locks
        if (!allLocks) {
            // Keep only session locks (owner == NULL)
            process_session_locks(locallock);
            if (has_session_locks(locallock))
                continue; // Keep this lock
        }

        // Handle fast-path locks (relation locks)
        if (locallock->proclock == NULL || locallock->lock == NULL) {
            if (!have_fast_path_lwlock) {
                LWLockAcquire(&MyProc->fpInfoLock, LW_EXCLUSIVE);
                have_fast_path_lwlock = true;
            }

            // Try fast-path release
            if (FastPathUnGrantRelationLock(relid, lockmode)) {
                RemoveLocalLock(locallock);
                continue;
            }

            // Fast-path failed, need to use main lock table
            LWLockRelease(&MyProc->fpInfoLock);
            have_fast_path_lwlock = false;
            LockRefindAndRelease(lockMethodTable, MyProc,
                               &locallock->tag.lock, lockmode, false);
        } else {
            // Mark normal locks for release
            if (locallock->nLocks > 0)
                locallock->proclock->releaseMask |= LOCKBIT_ON(locallock->tag.mode);
        }

        RemoveLocalLock(locallock);
    }

    // Release fast-path lock if still held
    if (have_fast_path_lwlock)
        LWLockRelease(&MyProc->fpInfoLock);

    // Phase 2: Process shared lock table by partition
    for (partition = 0; partition < NUM_LOCK_PARTITIONS; partition++)
    {
        dlist_head *procLocks = &MyProc->myProcLocks[partition];

        // Skip empty partitions for efficiency
        if (dlist_is_empty(procLocks))
            continue;

        LWLock *partitionLock = LockHashPartitionLockByIndex(partition);
        LWLockAcquire(partitionLock, LW_EXCLUSIVE);

        // Process each proclock in this partition
        dlist_foreach_modify(proclock_iter, procLocks)
        {
            PROCLOCK *proclock = dlist_container(PROCLOCK, procLink, proclock_iter.cur);
            lock = proclock->tag.myLock;

            // Skip wrong lock method
            if (LOCK_LOCKMETHOD(*lock) != lockmethodid)
                continue;

            // Set release mask for all locks if allLocks mode
            if (allLocks)
                proclock->releaseMask = proclock->holdMask;

            // Skip if nothing to release
            if (proclock->releaseMask == 0 && proclock->holdMask != 0)
                continue;

            // Release each marked lock mode
            bool wakeupNeeded = false;
            for (int i = 1; i <= lockMethodTable->numLockModes; i++) {
                if (proclock->releaseMask & LOCKBIT_ON(i))
                    wakeupNeeded |= UnGrantLock(lock, i, proclock, lockMethodTable);
            }

            proclock->releaseMask = 0;

            // Clean up and wake waiters if needed
            CleanUpLock(lock, proclock, lockMethodTable,
                       LockTagHashCode(&lock->tag), wakeupNeeded);
        }

        LWLockRelease(partitionLock);
    }
}
```

Key simplifications made:
- Removed detailed error handling and debug code for clarity
- Consolidated session lock processing into helper function concepts
- Abstracted complex lock owner manipulation logic
- Simplified fast-path lock handling flow
- Removed extensive assertions and debug prints
- Consolidated similar processing patterns
- Added high-level comments explaining the two-phase approach
- Streamlined partition iteration and proclock processing