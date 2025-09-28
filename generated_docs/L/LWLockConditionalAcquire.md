# LWLockConditionalAcquire

## Location
[src/backend/storage/lmgr/lwlock.c:1341-1397](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lwlock.c#L1341-L1397)

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
  - [LWLockAttemptLock](LWLockAttemptLock.md)
  - HOLD_INTERRUPTS (on success)
  - RESUME_INTERRUPTS (on failure)
  - PRINT_LWDEBUG, LOG_LWDEBUG (debug builds)
  - TRACE_POSTGRESQL_LWLOCK_CONDACQUIRE* (tracing enabled)
  - T_NAME (tracing enabled)
- Called from (representative examples):
  - [ss_report_location](../s/ss_report_location.md)
  - [TransactionIdSetPageStatus](../T/TransactionIdSetPageStatus.md)
  - [SimpleLruWaitIO](../S/SimpleLruWaitIO.md)
  - [XLogNeedsFlush](../X/XLogNeedsFlush.md)
  - [GetVictimBuffer](../G/GetVictimBuffer.md)
  - [ConditionalLockBuffer](../C/ConditionalLockBuffer.md)
  - [ProcArrayEndTransaction](../P/ProcArrayEndTransaction.md)
  - [pgstat_flush_io](../p/pgstat_flush_io.md)
  - [pgstat_lock_entry](../p/pgstat_lock_entry.md)
  - [pgstat_lock_entry_shared](../p/pgstat_lock_entry_shared.md)
  - [pgstat_slru_flush](../p/pgstat_slru_flush.md)
  - [pgstat_flush_wal](../p/pgstat_flush_wal.md)

## Notes and Other Information
- No queuing or sleeping occurs - the function returns immediately regardless of lock availability
- Interrupt handling is only modified if the lock is successfully acquired
- On failure, the function has no side effects and can be called repeatedly
- Commonly used in performance-critical code paths where blocking is unacceptable
- Useful for implementing lock hierarchies and deadlock avoidance strategies
- The function enforces the same maximum simultaneous lock limit as the blocking version
- DTrace/SystemTap tracing distinguishes between successful and failed conditional acquisitions
- Often used in conjunction with retry logic or alternative code paths when locks are unavailable

## Simplified Source

```c
// Simplified version of LWLockConditionalAcquire
bool LWLockConditionalAcquire(LWLock *lock, LWLockMode mode) {
    bool lock_acquired;

    // Validate input parameters
    Assert(mode == LW_SHARED || mode == LW_EXCLUSIVE);

    // Check we have room for another lock in our tracking array
    if (num_held_lwlocks >= MAX_SIMUL_LWLOCKS)
        elog(ERROR, "too many LWLocks taken");

    // Protect shared memory operations from interrupts
    HOLD_INTERRUPTS();

    // Try to acquire the lock without waiting
    lock_acquired = !LWLockAttemptLock(lock, mode);

    if (lock_acquired) {
        // Success: Add lock to our tracking list
        held_lwlocks[num_held_lwlocks].lock = lock;
        held_lwlocks[num_held_lwlocks].mode = mode;
        num_held_lwlocks++;

        // Note: Interrupts remain held until lock release
    } else {
        // Failure: Restore interrupt handling immediately
        RESUME_INTERRUPTS();
    }

    return lock_acquired;
}
```

Key simplifications made:
- Removed debug logging and tracing code for clarity
- Consolidated the mustwait variable logic into direct boolean handling
- Simplified the success/failure flow into clear if/else branches
- Abstracted the complex interrupt handling rationale into comments
- Removed platform-specific tracing macros
- Made the return logic more explicit and readable