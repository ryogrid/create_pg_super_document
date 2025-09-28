# LWLockQueueSelf

## Location
[src/backend/storage/lmgr/lwlock.c:1038-1080](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lwlock.c#L1038-L1080)

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
  - [LWLockWaitListLock](LWLockWaitListLock.md)
  - [pg_atomic_fetch_or_u32](../p/pg_atomic_fetch_or_u32.md)
  - proclist_push_head
  - proclist_push_tail
  - [LWLockWaitListUnlock](LWLockWaitListUnlock.md)
  - [pg_atomic_fetch_add_u32](../p/pg_atomic_fetch_add_u32.md) (debug builds only)
- Called from (representative examples):
  - [LWLockAcquire](LWLockAcquire.md)
  - [LWLockAcquireOrWait](LWLockAcquireOrWait.md)
  - [LWLockWaitForVar](LWLockWaitForVar.md)

## Notes and Other Information
- The function panics if MyProc is NULL or if the process is already waiting for another lock
- LW_WAIT_UNTIL_FREE waiters are placed at the front of the queue to ensure they are notified first when the lock becomes free
- The LW_FLAG_HAS_WAITERS flag is set atomically to indicate that the lock has processes waiting
- In debug builds, the function maintains a counter of waiting processes
- The wait list is protected by a spinlock to ensure atomic updates to the queue structure

## Simplified Source

```c
// Simplified version of LWLockQueueSelf
static void
LWLockQueueSelf(LWLock *lock, LWLockMode mode)
{
    // Safety checks: ensure we have a valid process structure
    if (MyProc == NULL)
        elog(PANIC, "cannot wait without a PGPROC structure");

    if (MyProc->lwWaiting != LW_WS_NOT_WAITING)
        elog(PANIC, "queueing for lock while waiting on another one");

    // Acquire wait list lock to safely modify the queue
    LWLockWaitListLock(lock);

    // Mark lock as having waiters
    pg_atomic_fetch_or_u32(&lock->state, LW_FLAG_HAS_WAITERS);

    // Set process wait state
    MyProc->lwWaiting = LW_WS_WAITING;
    MyProc->lwWaitMode = mode;

    // Add to queue: LW_WAIT_UNTIL_FREE goes to front, others to back
    if (mode == LW_WAIT_UNTIL_FREE)
        proclist_push_head(&lock->waiters, MyProcNumber, lwWaitLink);
    else
        proclist_push_tail(&lock->waiters, MyProcNumber, lwWaitLink);

    // Release wait list lock
    LWLockWaitListUnlock(lock);
}
```

Key simplifications made:
- Removed debug-only waiter counting code
- Consolidated safety checks with clear comments
- Simplified queue positioning logic explanation
- Abstracted atomic operations with descriptive comments
- Maintained essential algorithm flow and correctness