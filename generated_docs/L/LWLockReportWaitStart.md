# LWLockReportWaitStart

## Location
[src/backend/storage/lmgr/lwlock.c:727-735](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lwlock.c#L727-L735)

## Overview
Reports the start of a wait event for lightweight locks to PostgreSQL's statistics collection system.

## Definition
```c
static inline void LWLockReportWaitStart(LWLock *lock)
```

## Detailed Description
This static inline function serves as a specialized wrapper for reporting the beginning of lightweight lock wait events to PostgreSQL's wait event monitoring infrastructure. It is a crucial component for database performance monitoring and troubleshooting, allowing administrators to track when processes are waiting on specific lightweight locks.

The function constructs a wait event identifier by combining the PG_WAIT_LWLOCK base category with the specific tranche ID of the lock being waited on. This provides detailed visibility into which type of lightweight lock is causing wait contention.

Key characteristics:
- Declared as static inline for performance optimization (no function call overhead)
- Used consistently across all lightweight lock operations that involve waiting
- Integrates with PostgreSQL's comprehensive wait event monitoring system
- Enables tranche-specific wait event identification

## Parameters / Member Variables
- `lock`: Pointer to the LWLock structure for which the wait is starting

## Dependencies
- Functions called/Symbols referenced:
  - [LWLock](LWLock.md) (struct type)
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md) (statistics reporting function)
  - PG_WAIT_LWLOCK (wait event category constant)
- Called from (representative examples):
  - [LWLockAcquire](LWLockAcquire.md) (when entering wait state during lock acquisition)
  - [LWLockAcquireOrWait](LWLockAcquireOrWait.md) (when waiting becomes necessary)
  - [LWLockWaitForVar](LWLockWaitForVar.md) (when waiting for variable changes)

## Notes and Other Information
- Inline function designed for minimal performance overhead during lock contention
- The wait event type combines PG_WAIT_LWLOCK with the lock's tranche ID for specific identification
- Essential for PostgreSQL's wait event monitoring - enables identification of lock contention bottlenecks
- Must be paired with LWLockReportWaitEnd() to properly bracket wait periods
- Used internally by lightweight lock implementation - not intended for direct external use
- Tranche-based reporting allows categorization of waits by lock purpose (buffer, WAL, etc.)