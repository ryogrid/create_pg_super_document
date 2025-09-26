# LWLockConditionalAcquire

## Location
src/backend/storage/lmgr/lwlock.c: 1341 - 1397

## Overview
Non-blocking version of lightweight lock acquisition that returns immediately with success or failure rather than waiting for the lock to become available.

## Definition

```c
structures in shared memory.
	 */
	HOLD_INTERRUPTS();
```
## Detailed Description
LWLockConditionalAcquire provides a non-blocking alternative to LWLockAcquire for scenarios where waiting is not acceptable or desirable. The function attempts to acquire the lock once and returns immediately:

- If the lock is available, it acquires the lock and returns true
- If the lock is not available, it returns false without any side effects

This function is particularly useful in deadlock avoidance scenarios, optimistic locking strategies, or when implementing timeout-based operations. The function maintains the same interrupt handling and lock tracking semantics as the blocking version when successful.

## Parameters / Member Variables
- : Pointer to the LWLock structure to acquire
- : Lock acquisition mode (LW_SHARED or LW_EXCLUSIVE)

**Return value**:
- : Lock was successfully acquired
- : Lock was not available and could not be acquired

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAttemptLock
  - HOLD_INTERRUPTS (on success)
  - RESUME_INTERRUPTS (on failure)
  - PRINT_LWDEBUG, LOG_LWDEBUG (debug builds)
  - TRACE_POSTGRESQL_LWLOCK_CONDACQUIRE* (tracing enabled)
  - T_NAME (tracing enabled)
- Called from (representative examples):
  - ss_report_location
  - TransactionIdSetPageStatus
  - SimpleLruWaitIO
  - XLogNeedsFlush
  - GetVictimBuffer
  - ConditionalLockBuffer
  - ProcArrayEndTransaction
  - pgstat_flush_io
  - pgstat_lock_entry
  - pgstat_lock_entry_shared
  - pgstat_slru_flush
  - pgstat_flush_wal

## Notes and Other Information
- No queuing or sleeping occurs - the function returns immediately regardless of lock availability
- Interrupt handling is only modified if the lock is successfully acquired
- On failure, the function has no side effects and can be called repeatedly
- Commonly used in performance-critical code paths where blocking is unacceptable
- Useful for implementing lock hierarchies and deadlock avoidance strategies
- The function enforces the same maximum simultaneous lock limit as the blocking version
- DTrace/SystemTap tracing distinguishes between successful and failed conditional acquisitions
- Often used in conjunction with retry logic or alternative code paths when locks are unavailable