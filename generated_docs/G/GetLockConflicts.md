# GetLockConflicts

## Location
[src/backend/storage/lmgr/lock.c:2904-3111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L2904-L3111)

## Overview
GetLockConflicts returns an array of VirtualTransactionIds of transactions currently holding locks that would conflict with a specified lock mode, checking both the shared lock table and fast-path locks.

## Definition
```c
VirtualTransactionId *GetLockConflicts(const LOCKTAG *locktag, LOCKMODE lockmode, int *countp)
```

## Detailed Description
This function provides comprehensive conflict detection for PostgreSQL's locking system by examining both the standard shared lock table and per-backend fast-path lock arrays. It identifies all transactions that currently hold locks that would conflict with the requested lock mode on the specified lock tag. The function is critical for lock waiting logic, recovery conflict resolution, and deadlock detection.

The implementation first checks for potential fast-path conflicts by examining each backend's fast-path array if the requested lock could conflict with relation locks held via fast-path. Then it searches the shared lock hash table for the specific lock object and examines all current lock holders. The function carefully avoids reporting the current transaction as a conflicting holder and filters out transactions that have already committed or aborted.

## Parameters / Member Variables
- `locktag`: Pointer to the LOCKTAG structure identifying the specific lock resource
- `lockmode`: The lock mode for which conflicts are being checked
- `countp`: Optional pointer to receive the count of conflicting transactions (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [LockTagHashCode](../L/LockTagHashCode.md): Computes hash code for the lock tag
  - LockHashPartitionLock: Determines the appropriate partition lock
  - ConflictsWithRelationFastPath: Checks if the lock could conflict with fast-path locks
  - FAST_PATH_GET_BITS: Macro to extract lock bits from fast-path slots
  - [hash_search_with_hash_value](../h/hash_search_with_hash_value.md): Searches for lock objects in shared hash table
  - GET_VXID_FROM_PGPROC: Macro to extract virtual transaction ID from PGPROC
  - VirtualTransactionIdIsValid/VirtualTransactionIdEquals: VXID utility functions
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease: Low-level locking primitives
  - dlist_foreach/dlist_container: Doubly-linked list iteration macros
- Called from (representative examples):
  - [ProcSleep](../P/ProcSleep.md): During lock waiting to identify blocking transactions
  - [ResolveRecoveryConflictWithLock](../R/ResolveRecoveryConflictWithLock.md): For resolving conflicts during hot standby recovery
  - [WaitForLockersMultiple](../W/WaitForLockersMultiple.md): When waiting for multiple lock holders to complete

## Notes and Other Information
- Returns a palloc'd array terminated with an invalid VXID
- [Result](../R/Result.md) may become outdated immediately due to concurrent lock activity
- Excludes the current transaction from the conflict list
- For hot standby mode, uses a static array in TopMemoryContext for efficiency
- Handles both fast-path and standard lock table entries to provide complete coverage
- Includes logic to avoid duplicate entries when a transaction appears in both fast-path and standard tables
- The function performs database-level filtering to optimize fast-path scanning
- Transactions without valid lxid are considered non-conflicting (post-commit state)
- Includes panic-level error checking for impossible conditions like too many conflicts

## Simplified Source

```c
VirtualTransactionId *
GetLockConflicts(const LOCKTAG *locktag, LOCKMODE lockmode, int *countp)
{
    static VirtualTransactionId *vxids;
    LOCKMETHODID lockmethodid = locktag->locktag_lockmethodid;
    LockMethod lockMethodTable;
    LOCK *lock;
    LOCKMASK conflictMask;
    int count = 0;
    int fast_count = 0;

    // Validate lock method and mode
    if (lockmethodid <= 0 || lockmethodid >= lengthof(LockMethods))
        elog(ERROR, "unrecognized lock method: %d", lockmethodid);
    lockMethodTable = LockMethods[lockmethodid];
    if (lockmode <= 0 || lockmode > lockMethodTable->numLockModes)
        elog(ERROR, "unrecognized lock mode: %d", lockmode);

    // Allocate result array
    if (InHotStandby) {
        if (vxids == NULL)
            vxids = MemoryContextAlloc(TopMemoryContext,
                sizeof(VirtualTransactionId) * (MaxBackends + max_prepared_xacts + 1));
    } else {
        vxids = palloc0(sizeof(VirtualTransactionId) * (MaxBackends + max_prepared_xacts + 1));
    }

    // Get hash code and conflict mask
    uint32 hashcode = LockTagHashCode(locktag);
    LWLock *partitionLock = LockHashPartitionLock(hashcode);
    conflictMask = lockMethodTable->conflictTab[lockmode];

    // Check fast-path locks for relation conflicts
    if (ConflictsWithRelationFastPath(locktag, lockmode))
    {
        Oid relid = locktag->locktag_field2;

        // Scan all backends for fast-path conflicts
        for (int i = 0; i < ProcGlobal->allProcCount; i++)
        {
            PGPROC *proc = &ProcGlobal->allProcs[i];

            if (proc == MyProc) continue;  // Skip self

            LWLockAcquire(&proc->fpInfoLock, LW_SHARED);

            // Check database match
            if (proc->databaseId != locktag->locktag_field1) {
                LWLockRelease(&proc->fpInfoLock);
                continue;
            }

            // Check fast-path slots for conflicts
            for (uint32 f = 0; f < FP_LOCK_SLOTS_PER_BACKEND; f++)
            {
                if (relid != proc->fpRelId[f]) continue;

                uint32 lockmask = FAST_PATH_GET_BITS(proc, f);
                if (!lockmask) continue;

                lockmask <<= FAST_PATH_LOCKNUMBER_OFFSET;
                if ((lockmask & conflictMask) == 0) break;

                // Found conflict - add to result
                VirtualTransactionId vxid;
                GET_VXID_FROM_PGPROC(vxid, *proc);
                if (VirtualTransactionIdIsValid(vxid))
                    vxids[count++] = vxid;
                break;
            }

            LWLockRelease(&proc->fpInfoLock);
        }
    }

    fast_count = count;

    // Check standard lock table
    LWLockAcquire(partitionLock, LW_SHARED);

    lock = hash_search_with_hash_value(LockMethodLockHash, locktag, hashcode, HASH_FIND, NULL);
    if (!lock) {
        // No lock object exists
        LWLockRelease(partitionLock);
        vxids[count] = (VirtualTransactionId) {INVALID_PROC_NUMBER, InvalidLocalTransactionId};
        if (countp) *countp = count;
        return vxids;
    }

    // Examine all lock holders for conflicts
    dlist_iter proclock_iter;
    dlist_foreach(proclock_iter, &lock->procLocks)
    {
        PROCLOCK *proclock = dlist_container(PROCLOCK, lockLink, proclock_iter.cur);

        if (conflictMask & proclock->holdMask)
        {
            PGPROC *proc = proclock->tag.myProc;

            if (proc != MyProc)  // Skip self
            {
                VirtualTransactionId vxid;
                GET_VXID_FROM_PGPROC(vxid, *proc);

                if (VirtualTransactionIdIsValid(vxid))
                {
                    // Avoid duplicates from fast-path scan
                    bool duplicate = false;
                    for (int i = 0; i < fast_count; ++i) {
                        if (VirtualTransactionIdEquals(vxids[i], vxid)) {
                            duplicate = true;
                            break;
                        }
                    }
                    if (!duplicate)
                        vxids[count++] = vxid;
                }
            }
        }
    }

    LWLockRelease(partitionLock);

    // Terminate array and return
    vxids[count] = (VirtualTransactionId) {INVALID_PROC_NUMBER, InvalidLocalTransactionId};
    if (countp) *countp = count;
    return vxids;
}
```