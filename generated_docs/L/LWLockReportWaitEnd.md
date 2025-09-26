# LWLockReportWaitEnd

## Location
src/backend/storage/lmgr/lwlock.c: 736 - 744

## Overview
Reports the end of a wait event for lightweight locks to PostgreSQL's statistics collection system.

## Definition
```c
static inline void LWLockReportWaitEnd(void)
```

## Detailed Description
This static inline function serves as a lightweight wrapper for reporting the completion of lightweight lock wait events to PostgreSQL's wait event monitoring infrastructure. It complements LWLockReportWaitStart() to provide complete bracketing of wait periods, enabling accurate measurement of lock contention duration.

The function is designed to be the counterpart to LWLockReportWaitStart(), completing the wait event reporting cycle. Unlike its start counterpart, this function requires no parameters since it simply signals the end of the currently active wait event that was previously started.

Key characteristics:
- Declared as static inline for performance optimization (no function call overhead)
- Simple wrapper around the core statistics reporting function
- Used consistently across all lightweight lock operations that involve waiting
- Essential for accurate wait time measurement in PostgreSQL's monitoring system

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_report_wait_end (statistics reporting function)
- Called from (representative examples):
  - LWLockAcquire (when exiting wait state after lock acquisition)
  - LWLockAcquireOrWait (when wait period completes)
  - LWLockWaitForVar (when variable wait completes)

## Notes and Other Information
- Inline function designed for minimal performance overhead during lock operations
- Must be called after LWLockReportWaitStart() to properly complete wait event reporting
- Does not need lock-specific information since it ends the currently active wait event
- Critical for PostgreSQL's performance monitoring - enables measurement of actual wait durations
- Used internally by lightweight lock implementation - not intended for direct external use
- Paired usage with LWLockReportWaitStart() allows PostgreSQL to track both wait frequency and duration
- Failure to call this function after LWLockReportWaitStart() would leave wait events improperly tracked