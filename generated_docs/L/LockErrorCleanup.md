# LockErrorCleanup

## Location
src/backend/storage/lmgr/proc.c: 735 - 810

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
  - AbortStrongLockAcquire
  - RESUME_INTERRUPTS
  - disable_timeouts
  - LockHashPartitionLock
  - LWLockAcquire
  - dlist_node_is_detached
  - RemoveFromWaitQueue
  - GrantAwaitedLock
  - LWLockRelease
- Called from (representative examples):
  - AbortTransaction
  - AbortSubTransaction
  - ProcReleaseLocks
  - ProcessRecoveryConflictInterrupt
  - ProcessInterrupts

## Notes and Other Information
- The function preserves the LOCK_TIMEOUT indicator flag to distinguish between external SIGINT signals and lock timeout-generated signals
- Uses interrupt blocking (HOLD_INTERRUPTS/RESUME_INTERRUPTS) to ensure atomic cleanup
- Handles race conditions where the process may have been granted the lock or removed from the queue during cleanup
- Part of PostgreSQL's robust error recovery mechanism for lock management
- The lockAwaited global variable is cleared to indicate no pending lock wait