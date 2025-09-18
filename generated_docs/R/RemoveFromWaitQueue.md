# RemoveFromWaitQueue

## Location
src/backend/storage/lmgr/lock.c: 1908 - 1963

## Overview
RemoveFromWaitQueue removes a process from a lock's wait queue when the process has failed to acquire the lock, updating lock statistics and cleaning up associated state.

## Definition


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
  - dclist_delete_from_thoroughly (doubly-linked list operation)
  - LOCKBIT_OFF (macro)
  - CleanUpLock
  - Assert (assertion macro)
  - dclist_is_empty
  - lengthof (array length macro)
  - PROC_WAIT_STATUS_WAITING/PROC_WAIT_STATUS_ERROR (status constants)
- Called from (representative examples):
  - LockErrorCleanup
  - ProcSleep
  - CheckDeadLock
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