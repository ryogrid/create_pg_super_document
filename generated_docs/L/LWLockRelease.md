# LWLockRelease

## Location
[src/backend/storage/lmgr/lwlock.c:1783-1855](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lwlock.c#L1783-L1855)

## Overview
LWLockRelease releases a previously acquired lightweight lock and wakes up waiting processes as necessary.

## Definition

```c
void
LWLockRelease(LWLock *lock)
```
## Detailed Description
This function releases a lightweight lock that was previously acquired by the current process. It first locates the lock in the process's held_lwlocks array (searching backwards since locks are usually released in LIFO order), then atomically decrements the appropriate lock counter based on the lock mode.

The function uses atomic operations to release the lock state and carefully checks if waiters need to be awakened. Waiters are only awakened if the lock is completely free, has waiters, and the release flag is set. This optimization avoids unnecessary spinlock acquisition for wakeup operations when no waiters need notification.

After releasing the lock, the function may call LWLockWakeup() to notify waiting processes. Finally, it re-enables interrupts that were disabled during the lock acquisition to maintain consistency of shared memory structures.

## Parameters / Member Variables
- `*lock`: Pointer to the LWLock to release
## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_sub_fetch_u32](../p/pg_atomic_sub_fetch_u32.md) (atomic lock state updates)
  - [LWLockWakeup](LWLockWakeup.md) (waiter notification) 
  - RESUME_INTERRUPTS (interrupt management)
  - T_NAME (lock name for debugging)
  - LW_VAL_EXCLUSIVE/LW_VAL_SHARED (lock mode constants)
  - LW_FLAG_HAS_WAITERS/LW_FLAG_RELEASE_OK/LW_LOCK_MASK (state flags)
- Called from (representative examples):
  - No direct references found in the codebase analysis

## Notes and Other Information
- Searches held_lwlocks array backwards for efficiency (LIFO release pattern)
- Errors if the lock is not actually held by the current process
- Uses atomic operations to ensure thread-safe lock state updates
- Optimizes waiter wakeup by checking multiple conditions before acquiring spinlock
- Maintains the held_lwlocks array by compacting it after removal
- Includes comprehensive tracing and debugging support
- The interrupt re-enabling balances the HOLD_INTERRUPTS() from lock acquisition
- Critical for proper lock lifecycle management in PostgreSQL's concurrency control

## Simplified Source

```c
void LWLockRelease(LWLock *lock)
{
    LWLockMode mode;
    uint32 oldstate;
    bool check_waiters;
    int i;

    // Find lock in held_lwlocks array (search backwards for efficiency)
    for (i = num_held_lwlocks; --i >= 0;)
        if (lock == held_lwlocks[i].lock)
            break;

    if (i < 0)
        elog(ERROR, "lock %s is not held", T_NAME(lock));

    mode = held_lwlocks[i].mode;

    // Remove from held locks array and compact
    num_held_lwlocks--;
    for (; i < num_held_lwlocks; i++)
        held_lwlocks[i] = held_lwlocks[i + 1];

    // Atomically release the lock state
    if (mode == LW_EXCLUSIVE)
        oldstate = pg_atomic_sub_fetch_u32(&lock->state, LW_VAL_EXCLUSIVE);
    else
        oldstate = pg_atomic_sub_fetch_u32(&lock->state, LW_VAL_SHARED);

    // Check if we need to wake up waiters
    if ((oldstate & (LW_FLAG_HAS_WAITERS | LW_FLAG_RELEASE_OK)) ==
        (LW_FLAG_HAS_WAITERS | LW_FLAG_RELEASE_OK) &&
        (oldstate & LW_LOCK_MASK) == 0)
        check_waiters = true;
    else
        check_waiters = false;

    // Wake up waiters if necessary
    if (check_waiters)
        LWLockWakeup(lock);

    // Re-enable interrupts
    RESUME_INTERRUPTS();
}
```