# LogAccessExclusiveLockPrepare

## Location
[src/backend/storage/ipc/standby.c:1440-1461](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/standby.c#L1440-L1461)

## Overview
Prepares for AccessExclusive lock logging by ensuring the current transaction has a valid transaction ID assigned.

## Definition

```c
void
LogAccessExclusiveLockPrepare(void)
```
## Detailed Description
LogAccessExclusiveLockPrepare ensures that the current transaction has been assigned a TransactionId before an AccessExclusive lock is acquired and logged. This preparation step is crucial for proper lock release handling on standby servers and addresses two critical race conditions in the Hot Standby system.

First, having a transaction ID ensures that transaction completion records (commit/abort) are not optimized away by RecordTransactionCommit() or RecordTransactionAbort(). These completion records are essential for recovery processes on standby servers to know when to release the associated locks.

Second, assigning the transaction ID before the lock is recorded in shared memory prevents a race condition where GetRunningTransactionLocks() might observe a lock associated with InvalidTransactionId, which would violate system invariants and cause assertion failures.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTransactionId](../G/GetCurrentTransactionId.md)
- Called from (representative examples):
  - [LockAcquireExtended](LockAcquireExtended.md)

## Notes and Other Information
- This function must be called before LogAccessExclusiveLock() to ensure proper transaction ID assignment
- The comment describes this as a "hack" but necessary for a corner case to avoid adding complexity to the main commit path
- Prevents race conditions with GetRunningTransactionLocks() that could see locks with invalid transaction IDs
- Essential for proper lock release tracking on standby servers during recovery
- The function call to GetCurrentTransactionId() is cast to void since the return value is not needed
- Located in src/backend/storage/ipc/standby.c:1440-1461

## Simplified Source

```c
// Simplified version of LogAccessExclusiveLockPrepare
void LogAccessExclusiveLockPrepare(void) {
    // Ensure transaction has an ID assigned for proper lock release tracking
    // This prevents two issues:
    // 1. Transaction completion records won't be optimized away
    // 2. Avoids race condition with InvalidTransactionId in shared memory
    GetCurrentTransactionId();
}
```

Key simplifications made:
- Condensed the detailed comment into essential points
- Removed the void cast since it's not functionally important
- Preserved the core purpose: ensuring transaction ID assignment
- Maintained the essential logic flow (single function call)