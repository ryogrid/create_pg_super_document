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
- : The LWLock to monitor for release
- : Pointer to atomic uint64 variable to monitor for changes  
- : Expected value to wait for change from
- : Output parameter receiving current value when it differs from oldval

## Dependencies
- Functions called/Symbols referenced:
  - LWLockConflictsWithVar (core conflict detection)
  - LWLockQueueSelf/LWLockDequeueSelf (wait queue management)
  - pg_atomic_fetch_or_u32 (setting release flag)
  - LWLockReportWaitStart/LWLockReportWaitEnd (wait reporting)
  - PGSemaphoreLock/PGSemaphoreUnlock (process synchronization)
  - HOLD_INTERRUPTS/RESUME_INTERRUPTS (interrupt management)
- Called from (representative examples):
  - WaitXLogInsertionsToFinish (WAL insertion coordination)

## Notes and Other Information
- Ignores shared lock holders - treats shared locks as 'free' for waiting purposes
- Uses comprehensive statistics tracking when LWLOCK_STATS is enabled
- Includes extensive debugging and tracing support
- The function is specifically designed for WAL insertion coordination scenarios
- Memory barrier considerations depend on caller providing appropriate barriers
- Handles spurious wakeups gracefully by re-evaluating conditions in a loop