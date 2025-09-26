# ProcLockWakeup

## Location
[src/backend/storage/lmgr/proc.c:1711-1758](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/proc.c#L1711-L1758)

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
  - [dclist_is_empty](../d/dclist_is_empty.md) (check if wait queue is empty)
  - dclist_foreach_modify (safely iterate through wait queue with modifications)
  - dlist_container (get PGPROC from list node)
  - [LockCheckConflicts](../L/LockCheckConflicts.md) (check for conflicts with existing holders)
  - [GrantLock](../G/GrantLock.md) (grant the lock to a process)
  - [ProcWakeup](ProcWakeup.md) (wake up the newly granted process)
  - LOCKBIT_ON (convert lock mode to bitmask)
  - PROC_WAIT_STATUS_OK (success status for awakened processes)

- Called from (representative examples):
  - [DeadLockCheck](../D/DeadLockCheck.md) (when resolving deadlocks by aborting waiters)
  - [CleanUpLock](../C/CleanUpLock.md) (when locks are released during transaction cleanup)

## Notes and Other Information
- Must be called with the appropriate lock partition lock held by the caller
- Uses a mutable iterator to safely modify the wait queue during traversal
- Only processes that can be granted locks without conflicts are awakened
- Maintains the aheadRequests mask to track conflicting lock modes from earlier waiters
- Returns immediately if the wait queue is empty
- Part of PostgreSQL's lock management subsystem's core wakeup protocol
- Located in src/backend/storage/lmgr/proc.c:1711-1758
- Critical for maintaining lock queue ordering and preventing both deadlocks and starvation