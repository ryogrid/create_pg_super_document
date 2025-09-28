# LWLockAcquireOrWait

## Location
[src/backend/storage/lmgr/lwlock.c:1398-1524](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lwlock.c#L1398-L1524)

## Overview
LWLockAcquireOrWait attempts to acquire a lightweight lock in the specified mode, but if the lock is not immediately available, it waits until the lock is released without actually acquiring it.

## Definition

```c
structures in shared memory.
	 */
	HOLD_INTERRUPTS();
```
## Detailed Description
This function implements a unique locking semantic where it tries to acquire a lock, but if the lock is held by another process, it waits for the lock to be released and then returns false without acquiring the lock. This is particularly useful in scenarios like WAL flushing where one backend can perform work (like flushing commit records) on behalf of many other backends.

The function uses a two-phase acquisition protocol similar to LWLockAcquire() to handle race conditions. It first attempts lock acquisition, and if that fails, queues itself as a waiter, then attempts acquisition again. If the second attempt also fails, it waits until awakened by the lock holder.

The function holds interrupts during the critical section to prevent corruption of shared memory data structures and maintains statistics for lock contention monitoring.

## Parameters / Member Variables
- : Pointer to the LWLock to acquire or wait for
- : Lock acquisition mode (LW_SHARED or LW_EXCLUSIVE)

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAttemptLock](LWLockAttemptLock.md)
  - [LWLockQueueSelf](LWLockQueueSelf.md)  
  - [LWLockDequeueSelf](LWLockDequeueSelf.md)
  - [LWLockReportWaitStart](LWLockReportWaitStart.md)/LWLockReportWaitEnd
  - [PGSemaphoreLock](../P/PGSemaphoreLock.md)/PGSemaphoreUnlock
  - HOLD_INTERRUPTS/RESUME_INTERRUPTS
- Called from (representative examples):
  - [XLogFlush](../X/XLogFlush.md) (for WALWriteLock coordination)

## Notes and Other Information
- Returns true if lock was successfully acquired, false if had to wait
- Currently used primarily for WALWriteLock to allow multiple backends to benefit from a single WAL flush operation
- The function manages the held_lwlocks array when successfully acquiring the lock
- Includes comprehensive debugging and tracing support
- Statistics collection is available when LWLOCK_STATS is enabled

## Simplified Source

```c
// Simplified version of LWLockAcquireOrWait
bool LWLockAcquireOrWait(LWLock *lock, LWLockMode mode) {
    PGPROC *proc = MyProc;
    bool mustwait;
    int extraWaits = 0;

    Assert(mode == LW_SHARED || mode == LW_EXCLUSIVE);

    // Ensure we have room for the lock
    if (num_held_lwlocks >= MAX_SIMUL_LWLOCKS)
        elog(ERROR, "too many LWLocks taken");

    // Hold interrupts during critical section
    HOLD_INTERRUPTS();

    // First attempt to acquire the lock
    mustwait = LWLockAttemptLock(lock, mode);

    if (mustwait) {
        // Queue ourselves as a waiter
        LWLockQueueSelf(lock, LW_WAIT_UNTIL_FREE);

        // Second attempt to acquire the lock
        mustwait = LWLockAttemptLock(lock, mode);

        if (mustwait) {
            // Wait until awakened by lock holder
            LWLockReportWaitStart(lock);

            for (;;) {
                PGSemaphoreLock(proc->sem);
                if (proc->lwWaiting == LW_WS_NOT_WAITING)
                    break;
                extraWaits++;
            }

            LWLockReportWaitEnd();
        } else {
            // Got lock on second attempt, undo queueing
            LWLockDequeueSelf(lock);
        }
    }

    // Fix semaphore count for absorbed wakeups
    while (extraWaits-- > 0)
        PGSemaphoreUnlock(proc->sem);

    if (mustwait) {
        // Failed to get lock, release interrupt holdoff
        RESUME_INTERRUPTS();
        return false;
    } else {
        // Successfully acquired lock, add to held locks array
        held_lwlocks[num_held_lwlocks].lock = lock;
        held_lwlocks[num_held_lwlocks++].mode = mode;
        return true;
    }
}
```

Key simplifications made:
- Preserved the unique acquire-or-wait semantics
- Maintained the two-phase acquisition protocol for race condition handling
- Kept the essential interrupt management and semaphore operations
- Focused on the core lock coordination logic without debug/stats code
- Retained proper held_lwlocks array management