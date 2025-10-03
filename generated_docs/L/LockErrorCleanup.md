# LockErrorCleanup

## Location
[src/backend/storage/lmgr/proc.c:735-810](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/proc.c#L735-L810)

## Overview
Cancels any pending lock wait when aborting a transaction and reverts any strong lock count acquisition for a lock being acquired, typically called during error handling or transaction abort scenarios.

## Definition

```c
void
LockErrorCleanup(void)
```
## Detailed Description
LockErrorCleanup is a critical error recovery function that handles cleanup when a process must abort its current lock acquisition attempt. This function is designed to handle scenarios where a transaction is being aborted due to cancellation, death interrupts, or error conditions that occur before or during lock waits.

The function performs several key operations:
1. Aborts any strong lock acquisition in progress
2. Disables deadlock and lock timeout timers while preserving timeout indicators
3. Removes the process from lock wait queues if still queued
4. Handles cases where the lock may have already been granted during cleanup
5. Clears the global lockAwaited state

The function uses interrupt handling to ensure atomic cleanup operations and prevent race conditions during the cleanup process.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - HOLD_INTERRUPTS
  - [AbortStrongLockAcquire](../A/AbortStrongLockAcquire.md)
  - RESUME_INTERRUPTS
  - [disable_timeouts](../d/disable_timeouts.md)
  - LockHashPartitionLock
  - [LWLockAcquire](LWLockAcquire.md)
  - [dlist_node_is_detached](../d/dlist_node_is_detached.md)
  - [RemoveFromWaitQueue](../R/RemoveFromWaitQueue.md)
  - [GrantAwaitedLock](../G/GrantAwaitedLock.md)
  - [LWLockRelease](LWLockRelease.md)
- Called from (representative examples):
  - [AbortTransaction](../A/AbortTransaction.md)
  - [AbortSubTransaction](../A/AbortSubTransaction.md)
  - [ProcReleaseLocks](../P/ProcReleaseLocks.md)
  - [ProcessRecoveryConflictInterrupt](../P/ProcessRecoveryConflictInterrupt.md)
  - [ProcessInterrupts](../P/ProcessInterrupts.md)

## Notes and Other Information
- The function preserves the LOCK_TIMEOUT indicator flag to distinguish between external SIGINT signals and lock timeout-generated signals
- Uses interrupt blocking (HOLD_INTERRUPTS/RESUME_INTERRUPTS) to ensure atomic cleanup
- Handles race conditions where the process may have been granted the lock or removed from the queue during cleanup
- Part of PostgreSQL's robust error recovery mechanism for lock management
- The lockAwaited global variable is cleared to indicate no pending lock wait

## Simplified Source

```c
// Simplified version of LockErrorCleanup
void LockErrorCleanup(void) {
    LWLock *partitionLock;
    DisableTimeoutParams timeouts[2];

    // Block interrupts for atomic cleanup
    HOLD_INTERRUPTS();

    // Abort any strong lock acquisition in progress
    AbortStrongLockAcquire();

    // Check if we were actually waiting for a lock
    if (lockAwaited == NULL) {
        RESUME_INTERRUPTS();
        return;
    }

    // Disable deadlock and lock timeout timers
    timeouts[0].id = DEADLOCK_TIMEOUT;
    timeouts[0].keep_indicator = false;
    timeouts[1].id = LOCK_TIMEOUT;
    timeouts[1].keep_indicator = true;  // Preserve lock timeout indicator
    disable_timeouts(timeouts, 2);

    // Remove ourselves from the wait queue
    partitionLock = LockHashPartitionLock(lockAwaited->hashcode);
    LWLockAcquire(partitionLock, LW_EXCLUSIVE);

    if (!dlist_node_is_detached(&MyProc->links)) {
        // Still in queue - remove ourselves
        RemoveFromWaitQueue(MyProc, lockAwaited->hashcode);
    } else {
        // Already removed from queue - check if lock was granted
        if (MyProc->waitStatus == PROC_WAIT_STATUS_OK) {
            GrantAwaitedLock();
        }
    }

    // Clear the awaited lock state
    lockAwaited = NULL;

    LWLockRelease(partitionLock);
    RESUME_INTERRUPTS();
}
```

Key simplifications made:
- Removed detailed comments while preserving essential logic
- Consolidated variable declarations
- Simplified the timeout handling structure
- Focused on the main cleanup workflow
- Maintained all critical error recovery functionality
- Preserved interrupt handling and race condition management