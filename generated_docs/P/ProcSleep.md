# ProcSleep

## Location
[src/backend/storage/lmgr/proc.c:1071-1682](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/proc.c#L1071-L1682)

## Overview
ProcSleep puts a process to sleep waiting for the specified lock, handling deadlock detection, recovery conflicts, and autovacuum cancellation during the wait.

## Definition
```c
ProcWaitStatus ProcSleep(LOCALLOCK *locallock, LockMethod lockMethodTable, bool dontWait)
```

## Detailed Description
ProcSleep is the core function that implements PostgreSQL's lock waiting mechanism. When a process cannot immediately acquire a lock, this function places the process in the lock's wait queue and suspends execution until the lock becomes available or an error occurs.

The function performs several critical operations:
1. **Queue positioning**: Determines where to insert the process in the lock's wait queue based on priority and deadlock avoidance
2. **Immediate grant check**: May grant the lock immediately if no conflicts exist ahead in the queue
3. **Deadlock detection**: Sets up timers for deadlock detection and handles early deadlock scenarios
4. **Recovery conflict handling**: Special handling for Hot Standby recovery conflicts
5. **Autovacuum cancellation**: Can send signals to cancel blocking autovacuum processes
6. **Detailed logging**: Provides comprehensive lock wait logging for debugging

The function uses a priority-based wait queue where processes that already hold conflicting locks are placed ahead of others to minimize deadlock situations.

## Parameters / Member Variables
- `locallock`: The local lock information structure containing lock details and process-specific data
- `lockMethodTable`: The lock method table defining conflict rules and lock behavior for this lock type
- `dontWait`: If true, returns immediately with error status rather than waiting; if false, waits for the lock

## Dependencies
- Functions called/Symbols referenced:
  - LockHashPartitionLock (get partition lock for this lock)
  - [RememberSimpleDeadLock](../R/RememberSimpleDeadLock.md) (record deadlock information)
  - [LockCheckConflicts](../L/LockCheckConflicts.md) (check for lock conflicts)
  - [GrantLock](../G/GrantLock.md) (grant the lock to the process)
  - [GrantAwaitedLock](../G/GrantAwaitedLock.md) (update local lock table when granted)
  - [RemoveFromWaitQueue](../R/RemoveFromWaitQueue.md) (remove process from wait queue)
  - [RecoveryInProgress](../R/RecoveryInProgress.md) (check if in recovery mode)
  - [CheckRecoveryConflictDeadlock](../C/CheckRecoveryConflictDeadlock.md) (check for recovery conflicts)
  - [WaitLatch](../W/WaitLatch.md) (wait for latch to be set)
  - [CheckDeadLock](../C/CheckDeadLock.md) (run deadlock detection algorithm)
  - [GetBlockingAutoVacuumPgproc](../G/GetBlockingAutoVacuumPgproc.md) (find blocking autovacuum process)
  - [ResolveRecoveryConflictWithLock](../R/ResolveRecoveryConflictWithLock.md) (handle recovery conflicts)

- Called from (representative examples):
  - [WaitOnLock](../W/WaitOnLock.md) (main lock acquisition path)

## Notes and Other Information
- Returns PROC_WAIT_STATUS_OK if lock acquired, PROC_WAIT_STATUS_ERROR if failed or would block with dontWait=true
- Handles both regular backend processes and Hot Standby recovery scenarios differently
- Implements sophisticated autovacuum cancellation logic to avoid unnecessary blocking
- Provides extensive logging capabilities when log_lock_waits is enabled
- Uses timeout mechanisms for both deadlock detection and lock timeout enforcement
- The function can exit early if it detects the lock can be granted immediately
- Located in src/backend/storage/lmgr/proc.c:1071-1682
- Critical for PostgreSQL's concurrency control and deadlock prevention mechanisms