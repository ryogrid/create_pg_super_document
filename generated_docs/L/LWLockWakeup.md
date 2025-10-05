# LWLockWakeup

## Location
[src/backend/storage/lmgr/lwlock.c:922-1037](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lwlock.c#L922-L1037)

## Overview
Internal function that wakes up all waiting processes that currently have a chance to acquire a lightweight lock when the lock is being released.

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
LWLockWakeup is responsible for the complex process of waking up waiting processes when a lightweight lock is released. The function carefully manages which processes to wake up based on their wait modes to ensure proper lock semantics:

- For shared locks: Multiple shared lock waiters can be woken up simultaneously
- For exclusive locks: Only one exclusive lock waiter is woken up, and no further waiters are processed
- For LW_WAIT_UNTIL_FREE waiters: These are woken up but don't prevent further wakeups since they don't actually acquire the lock

The function maintains atomicity by using a two-phase approach: first collecting waiters to wake up while holding the wait list lock, then actually waking them up after releasing the lock state.

## Parameters / Member Variables
- : Pointer to the LWLock structure to wake up waiters for

## Dependencies
- Functions called/Symbols referenced:
  - [proclist_init](../p/proclist_init.md)
  - [LWLockWaitListLock](LWLockWaitListLock.md)
  - proclist_foreach_modify
  - GetPGProcByNumber
  - proclist_delete
  - proclist_push_tail
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md)
  - [pg_atomic_compare_exchange_u32](../p/pg_atomic_compare_exchange_u32.md)
  - pg_write_barrier
  - [PGSemaphoreUnlock](../P/PGSemaphoreUnlock.md)
  - LOG_LWDEBUG
- Called from (representative examples):
  - [LWLockRelease](LWLockRelease.md)

## Notes and Other Information
- The function uses a sophisticated state management system with atomic operations to ensure thread safety
- Waiters are moved to a temporary list during processing to avoid holding the wait list lock during the actual wakeup process
- The LW_FLAG_RELEASE_OK flag is managed to prevent additional releases until retrying processes get a chance to run
- Memory barriers ensure proper ordering of operations when unlinking processes from wait lists
- The function handles different wait modes (LW_EXCLUSIVE, LW_SHARED, LW_WAIT_UNTIL_FREE) with different wakeup strategies

## Simplified Source

```c
static void
LWLockWakeup(LWLock *lock)
{
    bool new_release_ok;
    bool wokeup_somebody = false;
    proclist_head wakeup;
    proclist_mutable_iter iter;

    proclist_init(&wakeup);
    new_release_ok = true;

    // Lock wait list while collecting backends to wake up
    LWLockWaitListLock(lock);

    proclist_foreach_modify(iter, &lock->waiters, lwWaitLink)
    {
        PGPROC *waiter = GetPGProcByNumber(iter.cur);

        // Stop if we already woke somebody and this is exclusive
        if (wokeup_somebody && waiter->lwWaitMode == LW_EXCLUSIVE)
            continue;

        // Move waiter from wait list to wakeup list
        proclist_delete(&lock->waiters, iter.cur, lwWaitLink);
        proclist_push_tail(&wakeup, iter.cur, lwWaitLink);

        if (waiter->lwWaitMode != LW_WAIT_UNTIL_FREE) {
            // Prevent additional wakeups until retryer gets to run
            new_release_ok = false;
            wokeup_somebody = true;
        }

        // Mark waiter as pending wakeup
        Assert(waiter->lwWaiting == LW_WS_WAITING);
        waiter->lwWaiting = LW_WS_PENDING_WAKEUP;

        // Only one exclusive lock can be woken up
        if (waiter->lwWaitMode == LW_EXCLUSIVE)
            break;
    }

    // Update lock state atomically
    uint32 old_state = pg_atomic_read_u32(&lock->state);
    while (true) {
        uint32 desired_state = old_state;

        // Set release OK flag based on what we woke up
        if (new_release_ok)
            desired_state |= LW_FLAG_RELEASE_OK;
        else
            desired_state &= ~LW_FLAG_RELEASE_OK;

        // Clear waiters flag if no more waiters
        if (proclist_is_empty(&wakeup))
            desired_state &= ~LW_FLAG_HAS_WAITERS;

        desired_state &= ~LW_FLAG_LOCKED;  // Release lock

        if (pg_atomic_compare_exchange_u32(&lock->state, &old_state, desired_state))
            break;
    }

    // Actually wake up the collected waiters
    proclist_foreach_modify(iter, &wakeup, lwWaitLink)
    {
        PGPROC *waiter = GetPGProcByNumber(iter.cur);
        proclist_delete(&wakeup, iter.cur, lwWaitLink);

        // Ensure proper memory ordering
        pg_write_barrier();
        waiter->lwWaiting = LW_WS_NOT_WAITING;
        PGSemaphoreUnlock(waiter->sem);
    }
}
```