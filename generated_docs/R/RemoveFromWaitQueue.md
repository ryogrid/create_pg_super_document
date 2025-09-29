# RemoveFromWaitQueue

## Location
[src/backend/storage/lmgr/lock.c:1908-1963](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L1908-L1963)

## Overview
RemoveFromWaitQueue removes a process from a lock's wait queue when the process has failed to acquire the lock, updating lock statistics and cleaning up associated state.

## Definition

```c
void
RemoveFromWaitQueue(PGPROC *proc, uint32 hashcode)
```
## Detailed Description
RemoveFromWaitQueue is responsible for cleanly removing a process from a lock's wait queue when the process cannot or should not continue waiting for the lock. This typically occurs when a deadlock is detected or when a conditional lock attempt fails.

The function performs several critical operations:
1. Removes the process from the lock's doubly-linked wait queue
2. Decrements the lock's request counters to reflect the cancelled wait
3. Updates the lock's wait mask if no other processes are waiting for the same lock mode
4. Sets the process's wait status to error state
5. Cleans up the process's wait-related fields
6. Calls CleanUpLock to handle proclock deletion and potentially wake other waiters

The function includes numerous assertions to verify the process is actually waiting and that data structures are in the expected state.

## Parameters / Member Variables
- : Pointer to the PGPROC structure representing the process to remove from the wait queue
- : Hash code identifying the lock partition (used for lock cleanup)

## Dependencies
- Functions called/Symbols referenced:
  - LOCK_LOCKMETHOD (macro)
  - [dclist_delete_from_thoroughly](../d/dclist_delete_from_thoroughly.md) (doubly-linked list operation)
  - LOCKBIT_OFF (macro)
  - [CleanUpLock](../C/CleanUpLock.md)
  - Assert (assertion macro)
  - [dclist_is_empty](../d/dclist_is_empty.md)
  - lengthof (array length macro)
  - PROC_WAIT_STATUS_WAITING/PROC_WAIT_STATUS_ERROR (status constants)
- Called from (representative examples):
  - [LockErrorCleanup](../L/LockErrorCleanup.md)
  - [ProcSleep](../P/ProcSleep.md)
  - [CheckDeadLock](../C/CheckDeadLock.md)
  - LockHashPartitionLockByProc

## Notes and Other Information
- Caller must hold the appropriate partition lock
- Caller is responsible for signaling the process if needed
- Does not clean up any LOCALLOCK objects that may exist for the lock
- The function sets waitStatus to PROC_WAIT_STATUS_ERROR to indicate failure
- Includes extensive assertions to verify data structure consistency
- May trigger cleanup that wakes other waiting processes via CleanUpLock
- Located in src/backend/storage/lmgr/lock.c at lines 1908-1963
- Critical for deadlock resolution and conditional lock failure handling

## Simplified Source

```c
// Simplified version of RemoveFromWaitQueue
void RemoveFromWaitQueue(PGPROC *proc, uint32 hashcode) {
    LOCK *waitLock = proc->waitLock;
    PROCLOCK *proclock = proc->waitProcLock;
    LOCKMODE lockmode = proc->waitLockMode;
    LOCKMETHODID lockmethodid = LOCK_LOCKMETHOD(*waitLock);

    // Verify process is actually waiting (assertions simplified)
    Assert(proc->waitStatus == PROC_WAIT_STATUS_WAITING);
    Assert(waitLock && proc->links.next != NULL);

    // Remove process from lock's wait queue
    dclist_delete_from_thoroughly(&waitLock->waitProcs, &proc->links);

    // Decrement lock request counters
    waitLock->nRequested--;
    waitLock->requested[lockmode]--;

    // Clear wait mask bit if no other processes waiting for this mode
    if (waitLock->granted[lockmode] == waitLock->requested[lockmode]) {
        waitLock->waitMask &= LOCKBIT_OFF(lockmode);
    }

    // Clear process wait state and mark as error
    proc->waitLock = NULL;
    proc->waitProcLock = NULL;
    proc->waitStatus = PROC_WAIT_STATUS_ERROR;

    // Clean up proclock and potentially wake other waiters
    CleanUpLock(waitLock, proclock, LockMethods[lockmethodid], hashcode, true);
}
```

Key simplifications made:
- Combined multiple assertions into simplified verification comments
- Removed detailed assertion checks for brevity while keeping essential ones
- Consolidated variable declarations at the top
- Added clear comments explaining each logical step
- Simplified the wait mask clearing logic explanation
- Maintained the essential algorithm flow and all critical operations