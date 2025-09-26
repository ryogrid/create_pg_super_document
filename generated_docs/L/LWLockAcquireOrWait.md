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