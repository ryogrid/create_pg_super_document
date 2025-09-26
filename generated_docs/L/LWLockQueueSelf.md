# LWLockQueueSelf

## Location
src/backend/storage/lmgr/lwlock.c: 1038 - 1080

## Overview
Internal function that adds the current process to the wait queue of a lightweight lock when the lock cannot be immediately acquired.

## Definition

```c
structure, there's no way to wait. This
	 * should never occur, since MyProc should only be null during shared
	 * memory initialization.
	 */
	if (MyProc == NULL)
		elog(PANIC, "cannot wait without a PGPROC structure");
```
## Detailed Description
LWLockQueueSelf handles the process of enqueueing the current process (MyProc) when it needs to wait for a lightweight lock. The function ensures proper queue ordering and state management:

- Sets up the process's wait state and wait mode
- Adds the process to the appropriate position in the wait queue
- Handles special positioning for LW_WAIT_UNTIL_FREE waiters (at front of queue)
- Maintains proper locking protocol with atomic operations

The function includes safety checks to prevent processes from queuing while already waiting for another lock, which would lead to deadlock situations.

## Parameters / Member Variables
- : Pointer to the LWLock structure to queue for
- : The lock mode being requested (LW_SHARED, LW_EXCLUSIVE, or LW_WAIT_UNTIL_FREE)

## Dependencies
- Functions called/Symbols referenced:
  - elog (with PANIC level)
  - LWLockWaitListLock
  - pg_atomic_fetch_or_u32
  - proclist_push_head
  - proclist_push_tail
  - LWLockWaitListUnlock
  - pg_atomic_fetch_add_u32 (debug builds only)
- Called from (representative examples):
  - LWLockAcquire
  - LWLockAcquireOrWait
  - LWLockWaitForVar

## Notes and Other Information
- The function panics if MyProc is NULL or if the process is already waiting for another lock
- LW_WAIT_UNTIL_FREE waiters are placed at the front of the queue to ensure they are notified first when the lock becomes free
- The LW_FLAG_HAS_WAITERS flag is set atomically to indicate that the lock has processes waiting
- In debug builds, the function maintains a counter of waiting processes
- The wait list is protected by a spinlock to ensure atomic updates to the queue structure