# LWLockWaitForVar

## Location
[src/backend/storage/lmgr/lwlock.c:1586-1721](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lwlock.c#L1586-L1721)

## Overview
LWLockWaitForVar waits until a lock is free or until a monitored atomic variable changes its value, providing sophisticated coordination for WAL insertion operations.

## Definition

```c
bool
LWLockWaitForVar(LWLock *lock, pg_atomic_uint64 *valptr, uint64 oldval,
				 uint64 *newval)
```
## Detailed Description
This function implements a complex waiting mechanism that monitors both lock state and an atomic variable value. It waits until either the lock becomes free (returns true) or the variable value changes (returns false with new value). This dual-condition waiting is essential for WAL insertion coordination where multiple backends wait for insertions to complete.

The function uses a loop-based approach with a two-phase locking protocol similar to LWLockAcquire(). It queues itself as a LW_WAIT_UNTIL_FREE waiter and sets the LW_FLAG_RELEASE_OK flag to ensure immediate notification when the lock is released. The function handles spurious wakeups by rechecking conditions after each wake event.

Special attention is paid to interrupt handling - interrupts are held during the waiting period to prevent corruption, as there's no cleanup mechanism to remove the process from wait queues if interrupted.

## Parameters / Member Variables
- `*lock`: The LWLock to monitor for release
- `*valptr`: Pointer to atomic uint64 variable to monitor for changes
- `oldval`: Expected value to wait for change from
- `*newval`: Output parameter receiving current value when it differs from oldval
## Dependencies
- Functions called/Symbols referenced:
  - [LWLockConflictsWithVar](LWLockConflictsWithVar.md) (core conflict detection)
  - [LWLockQueueSelf](LWLockQueueSelf.md)/LWLockDequeueSelf (wait queue management)
  - [pg_atomic_fetch_or_u32](../p/pg_atomic_fetch_or_u32.md) (setting release flag)
  - [LWLockReportWaitStart](LWLockReportWaitStart.md)/LWLockReportWaitEnd (wait reporting)
  - [PGSemaphoreLock](../P/PGSemaphoreLock.md)/PGSemaphoreUnlock (process synchronization)
  - HOLD_INTERRUPTS/RESUME_INTERRUPTS (interrupt management)
- Called from (representative examples):
  - [WaitXLogInsertionsToFinish](../W/WaitXLogInsertionsToFinish.md) (WAL insertion coordination)

## Notes and Other Information
- Ignores shared lock holders - treats shared locks as 'free' for waiting purposes
- Uses comprehensive statistics tracking when LWLOCK_STATS is enabled
- Includes extensive debugging and tracing support
- The function is specifically designed for WAL insertion coordination scenarios
- Memory barrier considerations depend on caller providing appropriate barriers
- Handles spurious wakeups gracefully by re-evaluating conditions in a loop

## Simplified Source

```c
// Simplified version of LWLockWaitForVar
bool LWLockWaitForVar(LWLock *lock, pg_atomic_uint64 *valptr, uint64 oldval, uint64 *newval) {
    PGPROC *proc = MyProc;
    int extra_wakeups = 0;
    bool result = false;

    // Prevent interrupts during waiting to avoid queue corruption
    HOLD_INTERRUPTS();

    // Main waiting loop
    for (;;) {
        bool must_wait;

        // Check if we need to wait (lock conflicts or variable unchanged)
        must_wait = LWLockConflictsWithVar(lock, valptr, oldval, newval, &result);

        if (!must_wait) {
            break;  // Lock is free or variable changed
        }

        // Add ourselves to the wait queue
        LWLockQueueSelf(lock, LW_WAIT_UNTIL_FREE);

        // Set flag to ensure we get woken up when lock is released
        pg_atomic_fetch_or_u32(&lock->state, LW_FLAG_RELEASE_OK);

        // Recheck conditions after queuing (race condition protection)
        must_wait = LWLockConflictsWithVar(lock, valptr, oldval, newval, &result);

        if (!must_wait) {
            // Condition satisfied after queuing - remove from queue
            LWLockDequeueSelf(lock);
            break;
        }

        // Wait until awakened by lock release or variable update
        LWLockReportWaitStart(lock);

        // Handle spurious wakeups by checking lwWaiting status
        for (;;) {
            PGSemaphoreLock(proc->sem);
            if (proc->lwWaiting == LW_WS_NOT_WAITING) {
                break;  // Actually woken up for our condition
            }
            extra_wakeups++;  // Spurious wakeup, count it
        }

        LWLockReportWaitEnd();

        // Continue outer loop to recheck conditions
    }

    // Fix semaphore count for any spurious wakeups
    while (extra_wakeups-- > 0) {
        PGSemaphoreUnlock(proc->sem);
    }

    // Re-enable interrupts
    RESUME_INTERRUPTS();

    return result;
}
```

Key simplifications made:
- Removed debug logging, statistics tracking, and tracing code
- Simplified variable names for clarity (extraWaits -> extra_wakeups)
- Consolidated comments to explain the core waiting logic
- Removed lock debug assertions and conditional compilation blocks
- Focused on the essential dual-condition waiting mechanism
- Maintained all critical synchronization and race condition handling
- Preserved interrupt management and semaphore correction logic