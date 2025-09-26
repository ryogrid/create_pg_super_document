# LWLockAcquire

## Location
[src/backend/storage/lmgr/lwlock.c:1170-1340](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lwlock.c#L1170-L1340)

## Overview
Primary public function for acquiring a lightweight lock in either shared or exclusive mode, blocking until the lock becomes available.

## Definition

```c
structures in shared memory.
	 */
	HOLD_INTERRUPTS();
```
## Detailed Description
LWLockAcquire is the main entry point for acquiring lightweight locks in PostgreSQL. It implements a sophisticated retry-based acquisition strategy to handle contention efficiently:

1. **Fast path**: Attempts immediate acquisition without queuing
2. **Queue and retry**: If fast path fails, queues the process and retries acquisition
3. **Sleep and wake**: If still unsuccessful, sleeps until signaled by lock release

The function includes important optimizations:
- Interrupt handling is disabled during lock holding to protect shared memory structures
- Multiple retry attempts before sleeping to avoid unnecessary context switches
- Proper semaphore management for spurious wakeups
- Lock tracking for debugging and proper cleanup

The design philosophy emphasizes efficiency for the common case where locks are held briefly and contention is low, while still handling high-contention scenarios correctly.

## Parameters / Member Variables
- : Pointer to the LWLock structure to acquire
- : Lock acquisition mode (LW_SHARED or LW_EXCLUSIVE)

**Return value**: 
- : Lock was acquired immediately without sleeping
- : Process had to sleep before acquiring the lock

## Dependencies
- Functions called/Symbols referenced:
  - [get_lwlock_stats_entry](../g/get_lwlock_stats_entry.md) (stats builds only)
  - HOLD_INTERRUPTS
  - [LWLockAttemptLock](LWLockAttemptLock.md)
  - [LWLockQueueSelf](LWLockQueueSelf.md)
  - [LWLockDequeueSelf](LWLockDequeueSelf.md)
  - [LWLockReportWaitStart](LWLockReportWaitStart.md)
  - [PGSemaphoreLock](../P/PGSemaphoreLock.md)
  - [pg_atomic_fetch_or_u32](../p/pg_atomic_fetch_or_u32.md)
  - [pg_atomic_fetch_sub_u32](../p/pg_atomic_fetch_sub_u32.md) (debug builds only)
  - [LWLockReportWaitEnd](LWLockReportWaitEnd.md)
  - [PGSemaphoreUnlock](../P/PGSemaphoreUnlock.md)
  - PRINT_LWDEBUG, LOG_LWDEBUG (debug builds)
  - TRACE_POSTGRESQL_LWLOCK_* (tracing enabled)
- Called from (representative examples):
  - Widely used throughout PostgreSQL for protecting shared data structures

## Notes and Other Information
- Interrupts are held off from acquisition until release to prevent corruption of shared memory
- The function maintains a list of held locks for proper cleanup and debugging
- Statistics tracking is available in LWLOCK_STATS builds
- DTrace/SystemTap tracing points are provided for performance analysis
- The retry loop handles race conditions where locks become available between checks
- Semaphore count is carefully managed to handle spurious wakeups correctly
- The function enforces a maximum number of simultaneously held locks per backend
- Critical for PostgreSQL's concurrency control and shared memory protection