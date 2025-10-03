# SetupLockInTable

## Location
[src/backend/storage/lmgr/lock.c:1183-1363](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L1183-L1363)

## Overview
SetupLockInTable creates or locates shared LOCK and PROCLOCK objects in PostgreSQL's lock table, establishing the necessary data structures for tracking lock ownership and requests.

## Definition

```c
static PROCLOCK *
SetupLockInTable(LockMethod lockMethodTable, PGPROC *proc,
				 const LOCKTAG *locktag, uint32 hashcode, LOCKMODE lockmode)
```
## Detailed Description
SetupLockInTable is a critical internal function that manages the shared memory structures necessary for lock tracking. It operates in two phases: first finding or creating a LOCK object that represents the lockable resource, then finding or creating a PROCLOCK object that represents the association between a process and that lock.

When creating new LOCK objects, it initializes all the necessary fields including grant/wait masks, request counters, and linked lists for tracking processes. For PROCLOCK objects, it establishes the connection between the process and lock, sets up group leadership relationships for parallel query support, and maintains proper linkage in both the lock's process list and the process's lock list.

The function includes sophisticated memory management, automatically cleaning up unused LOCK objects when PROCLOCK creation fails, preventing memory leaks in shared memory. It also includes optional deadlock risk detection code that can warn about potentially dangerous lock acquisition patterns.

## Parameters / Member Variables
- `lockMethodTable`: Pointer to the lock method configuration defining lock behavior and conflict rules
- `*proc`: Pointer to PGPROC structure representing the process acquiring the lock
- `*locktag`: Pointer to LOCKTAG structure uniquely identifying the lockable object
- `hashcode`: Pre-computed hash value for the lock tag for efficient hash table operations
- `lockmode`: The specific lock mode being requested for validation and tracking
## Dependencies
- Functions called/Symbols referenced:
  - [hash_search_with_hash_value](../h/hash_search_with_hash_value.md) (for finding/creating shared hash table entries)
  - [ProcLockHashCode](../P/ProcLockHashCode.md) (for computing proclock hash codes)
  - LockHashPartition (for determining lock partition)
  - [dlist_init](../d/dlist_init.md), dlist_push_tail (for managing linked lists)
  - [dclist_init](../d/dclist_init.md) (for wait queue initialization)
  - MemSet (for memory initialization)
- Data structures used:
  - [LOCK](../L/LOCK.md), PROCLOCK, PROCLOCKTAG (core lock structures)
  - LockMethodLockHash, LockMethodProcLockHash (shared hash tables)
- Called from (representative examples):
  - [LockAcquireExtended](../L/LockAcquireExtended.md) (main lock acquisition path)
  - [FastPathTransferRelationLocks](../F/FastPathTransferRelationLocks.md) (fast-path to shared table transfer)
  - [VirtualXactLock](../V/VirtualXactLock.md) (virtual transaction locking)

## Notes and Other Information
- Returns NULL on memory allocation failure, allowing callers to handle resource exhaustion gracefully
- Automatically garbage collects unused LOCK objects to prevent shared memory leaks
- Maintains bidirectional linkage between locks and processes for efficient traversal
- Includes compile-time deadlock risk detection code for debugging lock hierarchy violations
- Handles lock group leadership for parallel query coordination
- Critical for ensuring proper lock accounting and preventing race conditions in concurrent environments
- Must be called with appropriate partition lock held to ensure thread safety

## Simplified Source

```c
// Simplified version of SetupLockInTable
static PROCLOCK *
SetupLockInTable(LockMethod lockMethodTable, PGPROC *proc,
                 const LOCKTAG *locktag, uint32 hashcode, LOCKMODE lockmode)
{
    LOCK       *lock;
    PROCLOCK   *proclock;
    PROCLOCKTAG proclocktag;
    uint32      proclock_hashcode;
    bool        found;

    // Step 1: Find or create the LOCK object for this resource
    lock = hash_search_with_hash_value(LockMethodLockHash, locktag, hashcode,
                                       HASH_ENTER_NULL, &found);
    if (!lock)
        return NULL;  // Out of shared memory

    // Step 2: Initialize new LOCK if just created
    if (!found) {
        lock->grantMask = 0;
        lock->waitMask = 0;
        dlist_init(&lock->procLocks);
        dclist_init(&lock->waitProcs);
        lock->nRequested = 0;
        lock->nGranted = 0;
        // Clear request/grant counters for all lock modes
        MemSet(lock->requested, 0, sizeof(int) * MAX_LOCKMODES);
        MemSet(lock->granted, 0, sizeof(int) * MAX_LOCKMODES);
    }

    // Step 3: Create hash key for PROCLOCK (process-lock association)
    proclocktag.myLock = lock;
    proclocktag.myProc = proc;
    proclock_hashcode = ProcLockHashCode(&proclocktag, hashcode);

    // Step 4: Find or create the PROCLOCK object
    proclock = hash_search_with_hash_value(LockMethodProcLockHash, &proclocktag,
                                           proclock_hashcode, HASH_ENTER_NULL, &found);
    if (!proclock) {
        // Clean up unused LOCK if no other requestors
        if (lock->nRequested == 0) {
            hash_search_with_hash_value(LockMethodLockHash, &(lock->tag),
                                        hashcode, HASH_REMOVE, NULL);
        }
        return NULL;
    }

    // Step 5: Initialize new PROCLOCK if just created
    if (!found) {
        uint32 partition = LockHashPartition(hashcode);

        // Set group leader for parallel query support
        proclock->groupLeader = proc->lockGroupLeader ?
                               proc->lockGroupLeader : proc;
        proclock->holdMask = 0;
        proclock->releaseMask = 0;

        // Link into both lock's process list and process's lock list
        dlist_push_tail(&lock->procLocks, &proclock->lockLink);
        dlist_push_tail(&proc->myProcLocks[partition], &proclock->procLink);
    }

    // Step 6: Update request counters
    lock->nRequested++;
    lock->requested[lockmode]++;

    // Step 7: Verify we don't already hold this exact lock mode
    if (proclock->holdMask & LOCKBIT_ON(lockmode)) {
        elog(ERROR, "lock %s on object is already held",
             lockMethodTable->lockModeNames[lockmode]);
    }

    return proclock;
}
```

Key simplifications made:
- Removed detailed debug printing and assertions for clarity
- Consolidated error handling into essential checks only
- Removed optional deadlock risk detection code (CHECK_DEADLOCK_RISK)
- Simplified memory initialization operations with high-level comments
- Abstracted complex hash table operations with descriptive comments
- Condensed lock tag field references in error messages
- Focused on the core algorithm: find/create LOCK → find/create PROCLOCK → link structures