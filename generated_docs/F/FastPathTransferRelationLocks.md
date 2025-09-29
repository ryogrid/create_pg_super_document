# FastPathTransferRelationLocks

## Location
[src/backend/storage/lmgr/lock.c:2712-2799](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L2712-L2799)

## Overview
FastPathTransferRelationLocks transfers locks for a specific relation from all backends' per-backend fast-path arrays to the shared hash table, typically when fast-path optimization becomes inefficient due to lock conflicts.

## Definition
```c
static bool FastPathTransferRelationLocks(LockMethod lockMethodTable, const LOCKTAG *locktag, uint32 hashcode)
```

## Detailed Description
This function implements a critical transition mechanism in PostgreSQL's locking system. When the fast-path locking mechanism becomes ineffective (usually due to lock conflicts), this function migrates all existing fast-path locks for a specific relation to the standard shared lock table. It iterates through all active backends (ProcGlobal->allProcs), examines their fast-path slots, and transfers any matching relation locks to the shared hash table using the standard locking infrastructure.

The function ensures proper concurrency control by acquiring both the partition lock for the shared hash table and individual backend fast-path info locks. It also performs database-level filtering to avoid unnecessary processing of backends operating on different databases. After successfully transferring each lock, it clears the corresponding fast-path slot to prevent duplicate lock tracking.

## Parameters / Member Variables
- `lockMethodTable`: The lock method configuration defining lock conflict rules and behavior
- `locktag`: Lock tag structure containing the relation OID and database information for the target relation
- `hashcode`: Pre-computed hash value for the lock tag to determine the appropriate partition

## Dependencies
- Functions called/Symbols referenced:
  - LockHashPartitionLock: Determines the appropriate partition lock for the hash code
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease: Low-level locking primitives for concurrency control
  - FAST_PATH_GET_BITS: Macro to check if a fast-path slot is in use
  - FAST_PATH_CHECK_LOCKMODE: Macro to verify specific lock modes in fast-path slots
  - FAST_PATH_CLEAR_LOCKMODE: Macro to clear specific lock modes from fast-path slots
  - [SetupLockInTable](../S/SetupLockInTable.md): Creates or finds lock objects in the shared hash table
  - [GrantLock](../G/GrantLock.md): Grants the transferred lock in the shared lock table
  - FAST_PATH_LOCKNUMBER_OFFSET and FAST_PATH_BITS_PER_SLOT: Constants defining fast-path slot structure
- Called from (representative examples):
  - [LockAcquireExtended](../L/LockAcquireExtended.md): When fast-path optimization needs to be disabled
  - ConflictsWithRelationFastPath: When checking for potential fast-path conflicts

## Notes and Other Information
- Returns true on successful transfer, false if shared memory allocation fails
- Processes all backends in ProcGlobal->allProcs, excluding prepared transactions
- Uses database ID filtering to optimize processing by skipping irrelevant backends
- Acquires multiple locks in a specific order to prevent deadlocks: backend fpInfoLock first, then partition lock
- Clears fast-path slots after successful transfer to maintain consistency
- Critical for maintaining lock semantics when transitioning from optimized to standard locking
- Memory fencing considerations are noted in comments regarding database ID checks

## Simplified Source

```c
// Simplified version of FastPathTransferRelationLocks
static bool FastPathTransferRelationLocks(LockMethod lockMethodTable, const LOCKTAG *locktag, uint32 hashcode) {
    LWLock *partitionLock = LockHashPartitionLock(hashcode);
    Oid relid = locktag->locktag_field2;

    // Iterate through all active backends to find matching fast-path locks
    for (uint32 i = 0; i < ProcGlobal->allProcCount; i++) {
        PGPROC *proc = &ProcGlobal->allProcs[i];

        LWLockAcquire(&proc->fpInfoLock, LW_EXCLUSIVE);

        // Skip backends from different databases
        if (proc->databaseId != locktag->locktag_field1) {
            LWLockRelease(&proc->fpInfoLock);
            continue;
        }

        // Check each fast-path slot for matching relation
        for (uint32 slot = 0; slot < FP_LOCK_SLOTS_PER_BACKEND; slot++) {
            // Skip empty slots or non-matching relations
            if (relid != proc->fpRelId[slot] || FAST_PATH_GET_BITS(proc, slot) == 0) {
                continue;
            }

            // Transfer all lock modes from this slot to shared table
            LWLockAcquire(partitionLock, LW_EXCLUSIVE);
            for (uint32 lockmode = FAST_PATH_LOCKNUMBER_OFFSET;
                 lockmode < FAST_PATH_LOCKNUMBER_OFFSET + FAST_PATH_BITS_PER_SLOT;
                 lockmode++) {

                if (!FAST_PATH_CHECK_LOCKMODE(proc, slot, lockmode)) {
                    continue;
                }

                // Create lock entry in shared table
                PROCLOCK *proclock = SetupLockInTable(lockMethodTable, proc, locktag, hashcode, lockmode);
                if (!proclock) {
                    // Out of shared memory - cleanup and fail
                    LWLockRelease(partitionLock);
                    LWLockRelease(&proc->fpInfoLock);
                    return false;
                }

                // Grant the lock and clear from fast-path
                GrantLock(proclock->tag.myLock, proclock, lockmode);
                FAST_PATH_CLEAR_LOCKMODE(proc, slot, lockmode);
            }
            LWLockRelease(partitionLock);
            break; // Found the slot, no need to check remaining slots
        }
        LWLockRelease(&proc->fpInfoLock);
    }
    return true;
}
```

Key simplifications made:
- Removed detailed comments about memory fencing considerations
- Simplified variable declarations and loop structure
- Condensed the nested loop logic with clearer variable names
- Removed verbose comments while preserving essential algorithm steps
- Streamlined error handling to focus on the critical shared memory failure case
- Maintained the core lock transfer logic and proper lock acquisition order