# AtAbort_Twophase

## Location
[src/backend/access/transam/twophase.c:304-343](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L304-L343)

## Overview
Handles cleanup and unlocking of global transaction entries when a two-phase commit transaction is aborted or when a process terminates.

## Definition
void AtAbort_Twophase(void)

## Detailed Description
This function implements the abort logic for two-phase commit transactions, ensuring proper cleanup of shared memory resources and maintaining data consistency when transactions fail or processes terminate unexpectedly. It operates on the global variable MyLockedGxact, which tracks the global transaction entry currently locked by the process.

The function implements sophisticated logic to handle different abort scenarios:
1. **Pre-WAL abort**: If a transaction was being prepared but the WAL record wasn't written yet, the transaction is completely removed from shared memory since it was never truly prepared.
2. **Post-completion abort**: If a transaction fails after writing the final commit/rollback WAL record, it's also removed since it's no longer in a prepared state.
3. **Mid-process abort**: If abortion occurs after writing the initial prepare WAL record but before completion, the transaction entry remains in shared memory but is unlocked, allowing other processes to handle its completion.

The function uses exclusive locking on TwoPhaseStateLock to ensure atomic updates to the shared state and prevent race conditions during cleanup operations.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md) (acquires TwoPhaseStateLock in exclusive mode)
  - [LWLockRelease](../L/LWLockRelease.md) (releases TwoPhaseStateLock)
  - [RemoveGXact](../R/RemoveGXact.md) (removes invalid transaction entries from shared memory)
  - INVALID_PROC_NUMBER (constant indicating no backend owns the lock)
- Global variables accessed:
  - MyLockedGxact (current process's locked global transaction)
  - TwoPhaseStateLock (lightweight lock protecting two-phase state)
- Called from:
  - [AtProcExit_Twophase](AtProcExit_Twophase.md) (in twophase.c:297) - process exit cleanup
  - [AbortTransaction](AbortTransaction.md) (in xact.c:2862) - transaction abort handling

## Notes and Other Information
- Early return if MyLockedGxact is NULL (no locked transaction to clean up)
- The 'valid' flag on GlobalTransaction entries indicates whether the transaction is in a consistent prepared state
- Handles complex scenarios where WAL records may have been written but in-memory state is incomplete
- Critical for preventing resource leaks and maintaining database consistency during error conditions
- Part of PostgreSQL's robust error recovery mechanism for distributed transactions
- The function acknowledges that in some cases (post-WAL, pre-completion aborts), the in-memory state might be inconsistent, but it's too late to fully recover
- Thread-safe through proper use of LWLockAcquire/LWLockRelease around shared memory modifications