# ExpireAllKnownAssignedTransactionIds

## Location
[src/backend/storage/ipc/procarray.c:4497-4530](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L4497-L4530)

## Overview
Removes all entries from the KnownAssignedXids data structure and resets related transaction tracking state, effectively clearing all known assigned transactions during recovery shutdown.

## Definition
```c
void ExpireAllKnownAssignedTransactionIds(void)
```

## Detailed Description
This function performs a complete cleanup of the KnownAssignedXids tracking structure by removing all entries and resetting associated state variables. It is typically called during recovery shutdown or when transitioning between recovery states. The function resets latestCompletedXid to nextXid - 1, increments the transaction completion counter to reflect that all in-progress transactions are effectively aborted, and clears the lastOverflowedXid tracking variable. All operations are performed under exclusive ProcArrayLock to ensure consistency.

## Parameters / Member Variables
- This function takes no parameters

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md) (for ProcArrayLock exclusive access)
  - [KnownAssignedXidsRemovePreceding](../K/KnownAssignedXidsRemovePreceding.md) (removes all entries by passing InvalidTransactionId)
  - FullTransactionIdIsValid (validates nextXid before manipulation)
  - [FullTransactionIdRetreat](../F/FullTransactionIdRetreat.md) (decrements nextXid to get latestCompletedXid)
  - [LWLockRelease](../L/LWLockRelease.md) (releases ProcArrayLock)
- Called from (representative examples):
  - [ShutdownRecoveryTransactionEnvironment](../S/ShutdownRecoveryTransactionEnvironment.md) (during recovery shutdown)

## Notes and Other Information
- This function effectively treats all previously in-progress transactions as aborted
- The latestCompletedXid is set to nextXid - 1, representing the highest completed transaction
- Resets lastOverflowedXid for consistency with ExpireOldKnownAssignedTransactionIds behavior
- Uses exclusive locking on ProcArrayLock to ensure atomic updates to all related state
- Part of PostgreSQL's Hot Standby recovery infrastructure for managing transaction visibility on standby servers