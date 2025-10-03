# LWLockDequeueSelf

## Location
[src/backend/storage/lmgr/lwlock.c:1081-1169](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lwlock.c#L1081-L1169)

## Overview
Internal function that removes the current process from a lightweight lock's wait queue when it discovers it no longer needs to wait for the lock.

## Definition

```c
static void
LWLockDequeueSelf(LWLock *lock)
```
## Detailed Description
LWLockDequeueSelf handles the complex scenario where a process queued itself for a lock but later discovered it doesn't actually need to wait. This can happen due to race conditions where the lock becomes available between queuing and the final check. The function must handle two cases:

1. **Still on waitlist**: Remove the process cleanly from the queue
2. **Already removed by another process**: Handle the "superfluous wakeup" by consuming the semaphore signal that was sent

The function ensures proper cleanup of wait flags and semaphore states, maintaining consistency in the locking subsystem even when processes change their minds about waiting.

## Parameters / Member Variables
- `*lock`: Pointer to the LWLock structure to dequeue from
## Dependencies
- Functions called/Symbols referenced:
  - [get_lwlock_stats_entry](../g/get_lwlock_stats_entry.md) (stats builds only)
  - [LWLockWaitListLock](LWLockWaitListLock.md)
  - proclist_delete
  - [proclist_is_empty](../p/proclist_is_empty.md)
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md)
  - [pg_atomic_fetch_and_u32](../p/pg_atomic_fetch_and_u32.md)
  - [LWLockWaitListUnlock](LWLockWaitListUnlock.md)
  - [pg_atomic_fetch_or_u32](../p/pg_atomic_fetch_or_u32.md)
  - [PGSemaphoreLock](../P/PGSemaphoreLock.md)
  - [PGSemaphoreUnlock](../P/PGSemaphoreUnlock.md)
  - [pg_atomic_fetch_sub_u32](../p/pg_atomic_fetch_sub_u32.md) (debug builds only)
- Called from (representative examples):
  - [LWLockAcquire](LWLockAcquire.md)
  - [LWLockAcquireOrWait](LWLockAcquireOrWait.md)
  - [LWLockWaitForVar](LWLockWaitForVar.md)

## Notes and Other Information
- The function handles race conditions where another process may have already removed and signaled the current process
- When the process was already dequeued by someone else, it must consume the wakeup signal to maintain semaphore balance
- The LW_FLAG_RELEASE_OK flag is restored when handling superfluous wakeups to allow further lock releases
- Statistics tracking is included in LWLOCK_STATS builds to monitor dequeue frequency
- Debug builds maintain a waiter count that is decremented when the process stops waiting
- The function carefully manages the lwWaiting state transitions to prevent inconsistencies

## Simplified Source

```c
// Simplified version of LWLockDequeueSelf
static void
LWLockDequeueSelf(LWLock *lock)
{
    bool on_waitlist;

    // Lock the wait list to check and modify our waiting status
    LWLockWaitListLock(lock);

    // Check if we're still on the waitlist
    on_waitlist = MyProc->lwWaiting == LW_WS_WAITING;
    if (on_waitlist) {
        // Remove ourselves from the waiters queue
        proclist_delete(&lock->waiters, MyProcNumber, lwWaitLink);
    }

    // If no more waiters, clear the HAS_WAITERS flag
    if (proclist_is_empty(&lock->waiters) &&
        (pg_atomic_read_u32(&lock->state) & LW_FLAG_HAS_WAITERS) != 0) {
        pg_atomic_fetch_and_u32(&lock->state, ~LW_FLAG_HAS_WAITERS);
    }

    LWLockWaitListUnlock(lock);

    if (on_waitlist) {
        // We removed ourselves - clear waiting state
        MyProc->lwWaiting = LW_WS_NOT_WAITING;
    } else {
        // Someone else dequeued us - handle the wakeup signal

        // Restore RELEASE_OK flag that may have been cleared
        pg_atomic_fetch_or_u32(&lock->state, LW_FLAG_RELEASE_OK);

        // Wait for and consume the wakeup signal we received
        int extraWaits = 0;
        for (;;) {
            PGSemaphoreLock(MyProc->sem);
            if (MyProc->lwWaiting == LW_WS_NOT_WAITING)
                break;
            extraWaits++;
        }

        // Fix semaphore count for any extra signals absorbed
        while (extraWaits-- > 0)
            PGSemaphoreUnlock(MyProc->sem);
    }
}
```

Key simplifications made:
- Removed LWLOCK_STATS tracking code for clarity
- Removed LOCK_DEBUG waiter count management
- Simplified comments to focus on core logic flow
- Consolidated variable declarations
- Preserved essential race condition handling logic
- Maintained all critical atomic operations and semaphore management