# ReadNextFullTransactionId

## Location
src/backend/access/transam/varsup.c: 288 - 303

## Overview
ReadNextFullTransactionId provides read-only access to the next available transaction ID without allocating it.

## Definition
```c
FullTransactionId ReadNextFullTransactionId(void)
```

## Detailed Description
ReadNextFullTransactionId is a simple utility function that returns the current value of the next transaction ID counter from shared memory without advancing it. This function is used when components need to examine the current transaction ID state for informational purposes, such as during vacuum operations, conflict resolution, or monitoring. Unlike `GetNewTransactionId`, this function only reads the value and does not modify any shared state, making it safe for concurrent access with a shared lock.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - FullTransactionId
  - LW_SHARED
- Called from (representative examples):
  - gistdeletepage
  - _bt_unlink_halfdead_page
  - AdjustToFullTransactionId
  - XLogWalRcvSendHSFeedback
  - TransactionIdInRecentPast
  - ResolveRecoveryConflictWithSnapshotFullXid
  - pg_current_snapshot
  - ReadNextTransactionId

## Notes and Other Information
- Located in src/backend/access/transam/varsup.c:288-303
- Acquires XidGenLock with LW_SHARED for thread-safe reading
- Does not modify any shared state or advance transaction counters
- Commonly used in vacuum operations and conflict resolution
- Provides atomic read of the transaction counter value
- Essential for monitoring and diagnostic purposes
- Much lighter weight than GetNewTransactionId since it performs no allocations