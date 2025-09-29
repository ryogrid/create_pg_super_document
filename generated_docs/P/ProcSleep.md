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

## Simplified Source

```c
ProcWaitStatus ProcSleep(LOCALLOCK *locallock, LockMethod lockMethodTable, bool dontWait)
{
    LOCKMODE lockmode = locallock->tag.mode;
    LOCK *lock = locallock->lock;
    PROCLOCK *proclock = locallock->proclock;
    uint32 hashcode = locallock->hashcode;
    LWLock *partitionLock = LockHashPartitionLock(hashcode);
    dclist_head *waitQueue = &lock->waitProcs;
    PGPROC *insert_before = NULL;
    LOCKMASK myHeldLocks = MyProc->heldLocks;
    bool early_deadlock = false;
    ProcWaitStatus myWaitStatus;

    // Include group locking if applicable
    PGPROC *leader = MyProc->lockGroupLeader;
    if (leader != NULL) {
        // Add locks held by group members to myHeldLocks
        dlist_iter iter;
        dlist_foreach(iter, &lock->procLocks) {
            PROCLOCK *otherproclock = dlist_container(PROCLOCK, lockLink, iter.cur);
            if (otherproclock->groupLeader == leader)
                myHeldLocks |= otherproclock->holdMask;
        }
    }

    // Determine queue position and check for immediate grant
    if (myHeldLocks != 0 && !dclist_is_empty(waitQueue)) {
        LOCKMASK aheadRequests = 0;
        dlist_iter iter;

        dclist_foreach(iter, waitQueue) {
            PGPROC *proc = dlist_container(PGPROC, links, iter.cur);

            // Skip group members
            if (leader != NULL && leader == proc->lockGroupLeader)
                continue;

            // Check if this waiter must wait for me
            if (lockMethodTable->conflictTab[proc->waitLockMode] & myHeldLocks) {
                // Check if I must wait for this waiter
                if (lockMethodTable->conflictTab[lockmode] & proc->heldLocks) {
                    // Deadlock detected
                    RememberSimpleDeadLock(MyProc, lockmode, lock, proc);
                    early_deadlock = true;
                    break;
                }

                // Check if I can be granted immediately
                if ((lockMethodTable->conflictTab[lockmode] & aheadRequests) == 0 &&
                    !LockCheckConflicts(lockMethodTable, lockmode, lock, proclock)) {
                    // Grant lock immediately and return
                    GrantLock(lock, proclock, lockmode);
                    GrantAwaitedLock();
                    return PROC_WAIT_STATUS_OK;
                }

                // Insert before this conflicting process
                insert_before = proc;
                break;
            }
            aheadRequests |= LOCKBIT_ON(proc->waitLockMode);
        }
    }

    // Return early if told not to wait
    if (dontWait)
        return PROC_WAIT_STATUS_ERROR;

    // Insert into wait queue
    if (insert_before)
        dclist_insert_before(waitQueue, &insert_before->links, &MyProc->links);
    else
        dclist_push_tail(waitQueue, &MyProc->links);

    lock->waitMask |= LOCKBIT_ON(lockmode);

    // Set up wait information
    MyProc->waitLock = lock;
    MyProc->waitProcLock = proclock;
    MyProc->waitLockMode = lockmode;
    MyProc->waitStatus = PROC_WAIT_STATUS_WAITING;

    // Handle early deadlock
    if (early_deadlock) {
        RemoveFromWaitQueue(MyProc, hashcode);
        return PROC_WAIT_STATUS_ERROR;
    }

    lockAwaited = locallock;

    // Release partition lock before waiting
    LWLockRelease(partitionLock);

    // Check for recovery conflicts
    if (RecoveryInProgress() && !InRecovery)
        CheckRecoveryConflictDeadlock();

    // Set up deadlock and lock timeouts
    deadlock_state = DS_NOT_YET_CHECKED;
    got_deadlock_timeout = false;

    if (!InHotStandby) {
        // Enable deadlock timeout (and lock timeout if configured)
        if (LockTimeout > 0) {
            EnableTimeoutParams timeouts[2];
            timeouts[0].id = DEADLOCK_TIMEOUT;
            timeouts[0].type = TMPARAM_AFTER;
            timeouts[0].delay_ms = DeadlockTimeout;
            timeouts[1].id = LOCK_TIMEOUT;
            timeouts[1].type = TMPARAM_AFTER;
            timeouts[1].delay_ms = LockTimeout;
            enable_timeouts(timeouts, 2);
        } else {
            enable_timeout_after(DEADLOCK_TIMEOUT, DeadlockTimeout);
        }
        pg_atomic_write_u64(&MyProc->waitStart,
                           get_timeout_start_time(DEADLOCK_TIMEOUT));
    }

    // Main wait loop
    do {
        if (InHotStandby) {
            // Handle recovery conflicts
            ResolveRecoveryConflictWithLock(locallock->tag.lock, false);
        } else {
            // Regular wait
            WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0,
                     PG_WAIT_LOCK | locallock->tag.lock.locktag_type);
            ResetLatch(MyLatch);

            // Check for deadlock timeout
            if (got_deadlock_timeout) {
                CheckDeadLock();
                got_deadlock_timeout = false;
            }
            CHECK_FOR_INTERRUPTS();
        }

        // Check current wait status
        myWaitStatus = *((volatile ProcWaitStatus *) &MyProc->waitStatus);

        // Handle autovacuum cancellation if blocked by autovacuum
        if (deadlock_state == DS_BLOCKED_BY_AUTOVACUUM) {
            PGPROC *autovac = GetBlockingAutoVacuumPgproc();
            // [Autovacuum cancellation logic omitted for brevity]
        }

        // Log lock waits if enabled
        if (log_lock_waits && deadlock_state != DS_NOT_YET_CHECKED) {
            // [Detailed logging code omitted for brevity]
        }

    } while (myWaitStatus == PROC_WAIT_STATUS_WAITING);

    // Disable timeouts
    if (!InHotStandby) {
        if (LockTimeout > 0) {
            DisableTimeoutParams timeouts[2];
            timeouts[0].id = DEADLOCK_TIMEOUT;
            timeouts[0].keep_indicator = false;
            timeouts[1].id = LOCK_TIMEOUT;
            timeouts[1].keep_indicator = true;
            disable_timeouts(timeouts, 2);
        } else {
            disable_timeout(DEADLOCK_TIMEOUT, false);
        }
    }

    // Re-acquire partition lock
    LWLockAcquire(partitionLock, LW_EXCLUSIVE);

    // Clear lock awaited marker
    lockAwaited = NULL;

    // Update local lock table if we got the lock
    if (MyProc->waitStatus == PROC_WAIT_STATUS_OK)
        GrantAwaitedLock();

    return MyProc->waitStatus;
}
```