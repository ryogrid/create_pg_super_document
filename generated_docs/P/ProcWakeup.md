# ProcWakeup

## Location
src/backend/storage/lmgr/proc.c: 1683 - 1710

## Overview
ProcWakeup wakes up a sleeping process by setting its latch and removing it from the lock wait queue, signaling successful or failed lock acquisition.

## Definition
```c
void ProcWakeup(PGPROC *proc, ProcWaitStatus waitStatus)
```

## Detailed Description
ProcWakeup is responsible for awakening a process that has been waiting for a lock via ProcSleep. This function performs the essential cleanup and signaling operations required when a lock becomes available or when a waiting process needs to be awakened due to an error condition.

The function performs these key operations:
1. **Safety check**: Verifies the process is actually in a waiting state and hasn't already been awakened
2. **Queue removal**: Removes the process from the lock's wait queue using atomic list operations
3. **State cleanup**: Clears the process's wait-related fields and sets the final wait status
4. **Latch signaling**: Sets the process's latch to wake up the sleeping process

The function is designed to work correctly only for successful lock acquisition cases (PROC_WAIT_STATUS_OK). For failure cases, additional cleanup of lock request counts would be needed, which is handled by RemoveFromWaitQueue instead.

## Parameters / Member Variables
- `proc`: Pointer to the PGPROC structure of the process to be awakened
- `waitStatus`: The final wait status to set (typically PROC_WAIT_STATUS_OK for successful cases)

## Dependencies
- Functions called/Symbols referenced:
  - dlist_node_is_detached (check if process already removed from queue)
  - dclist_delete_from_thoroughly (remove from wait queue)
  - pg_atomic_write_u64 (atomically clear wait start time)
  - SetLatch (signal the sleeping process to wake up)
  - PROC_WAIT_STATUS_WAITING (expected current status)

- Called from (representative examples):
  - ProcLockWakeup (wake up processes when locks are released)

## Notes and Other Information
- Must be called with the appropriate lock partition lock held
- Currently only works correctly for the success case (PROC_WAIT_STATUS_OK)
- Includes safety checks to avoid double-wakeup scenarios
- Clears the waitStart timestamp atomically
- The latch mechanism is the core inter-process signaling primitive in PostgreSQL
- Part of the lock management subsystem's wakeup protocol
- Located in src/backend/storage/lmgr/proc.c:1683-1710
- Should not be used directly for error cases - use RemoveFromWaitQueue instead for proper cleanup