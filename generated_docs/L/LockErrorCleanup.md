# LockErrorCleanup

## Location
[src/backend/storage/lmgr/proc.c:735-810](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/proc.c#L735-L810)

## Overview
Cancels any pending lock wait when aborting a transaction and reverts any strong lock count acquisition for a lock being acquired, typically called during error handling or transaction abort scenarios.

## Definition


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
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - HOLD_INTERRUPTS
  - [AbortStrongLockAcquire](../A/AbortStrongLockAcquire.md)
  - RESUME_INTERRUPTS
  - disable_timeouts
  - LockHashPartitionLock
  - LWLockAcquire
  - [dlist_node_is_detached](../d/dlist_node_is_detached.md)
  - [RemoveFromWaitQueue](../R/RemoveFromWaitQueue.md)
  - [GrantAwaitedLock](../G/GrantAwaitedLock.md)
  - LWLockRelease
- Called from (representative examples):
  - [AbortTransaction](../A/AbortTransaction.md)
  - [AbortSubTransaction](../A/AbortSubTransaction.md)
  - [ProcReleaseLocks](../P/ProcReleaseLocks.md)
  - [ProcessRecoveryConflictInterrupt](../P/ProcessRecoveryConflictInterrupt.md)
  - ProcessInterrupts

## Notes and Other Information
- The function preserves the LOCK_TIMEOUT indicator flag to distinguish between external SIGINT signals and lock timeout-generated signals
- Uses interrupt blocking (HOLD_INTERRUPTS/RESUME_INTERRUPTS) to ensure atomic cleanup
- Handles race conditions where the process may have been granted the lock or removed from the queue during cleanup
- Part of PostgreSQL's robust error recovery mechanism for lock management
- The lockAwaited global variable is cleared to indicate no pending lock wait