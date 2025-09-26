# ProcLockWakeup

## Location
src/backend/storage/lmgr/proc.c: 1711 - 1758

## Overview
ProcLockWakeup wakes up processes waiting for a lock when the lock is released or a prior waiter is aborted, granting locks to all eligible waiters.

## Definition
```c
void ProcLockWakeup(LockMethod lockMethodTable, LOCK *lock)
```

## Detailed Description
ProcLockWakeup is the central function responsible for processing the wait queue when a lock becomes available. When a lock is released or a waiting process is aborted, this function scans through all processes waiting for that lock and determines which ones can now be granted the lock.

The function implements PostgreSQL's lock granting policy:
1. **Queue traversal**: Iterates through the wait queue in priority order
2. **Conflict checking**: For each waiter, checks if granting the lock would conflict with earlier waiters or existing lock holders
3. **Lock granting**: Awards the lock to processes that don't have conflicts
4. **Process awakening**: Wakes up newly granted processes via ProcWakeup
5. **Conflict tracking**: Maintains a mask of lock modes requested by conflicted waiters

The algorithm ensures fairness by only granting locks to processes that don't conflict with earlier waiters in the queue. This prevents starvation while maintaining the priority ordering established during initial queue insertion.

## Parameters / Member Variables
- `lockMethodTable`: The lock method table defining conflict rules and behavior for this lock type
- `lock`: Pointer to the LOCK structure containing the wait queue and lock state

## Dependencies
- Functions called/Symbols referenced:
  - dclist_is_empty (check if wait queue is empty)
  - dclist_foreach_modify (safely iterate through wait queue with modifications)
  - dlist_container (get PGPROC from list node)
  - LockCheckConflicts (check for conflicts with existing holders)
  - GrantLock (grant the lock to a process)
  - ProcWakeup (wake up the newly granted process)
  - LOCKBIT_ON (convert lock mode to bitmask)
  - PROC_WAIT_STATUS_OK (success status for awakened processes)

- Called from (representative examples):
  - DeadLockCheck (when resolving deadlocks by aborting waiters)
  - CleanUpLock (when locks are released during transaction cleanup)

## Notes and Other Information
- Must be called with the appropriate lock partition lock held by the caller
- Uses a mutable iterator to safely modify the wait queue during traversal
- Only processes that can be granted locks without conflicts are awakened
- Maintains the aheadRequests mask to track conflicting lock modes from earlier waiters
- Returns immediately if the wait queue is empty
- Part of PostgreSQL's lock management subsystem's core wakeup protocol
- Located in src/backend/storage/lmgr/proc.c:1711-1758
- Critical for maintaining lock queue ordering and preventing both deadlocks and starvation