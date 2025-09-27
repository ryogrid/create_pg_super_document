# ReadNextFullTransactionId

## Location
[src/backend/access/transam/varsup.c:288-303](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/varsup.c#L288-L303)

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
  - [FullTransactionId](../F/FullTransactionId.md)
  - LW_SHARED
- Called from (representative examples):
  - [gistdeletepage](../g/gistdeletepage.md)
  - [_bt_unlink_halfdead_page](../b/_bt_unlink_halfdead_page.md)
  - [AdjustToFullTransactionId](../A/AdjustToFullTransactionId.md)
  - [XLogWalRcvSendHSFeedback](../X/XLogWalRcvSendHSFeedback.md)
  - [TransactionIdInRecentPast](../T/TransactionIdInRecentPast.md)
  - [ResolveRecoveryConflictWithSnapshotFullXid](ResolveRecoveryConflictWithSnapshotFullXid.md)
  - [pg_current_snapshot](../p/pg_current_snapshot.md)
  - [ReadNextTransactionId](ReadNextTransactionId.md)

## Notes and Other Information
- Located in src/backend/access/transam/varsup.c:288-303
- Acquires XidGenLock with LW_SHARED for thread-safe reading
- Does not modify any shared state or advance transaction counters
- Commonly used in vacuum operations and conflict resolution
- Provides atomic read of the transaction counter value
- Essential for monitoring and diagnostic purposes
- Much lighter weight than GetNewTransactionId since it performs no allocations

## Simplified Source

```c
// Simplified version of ReadNextFullTransactionId
FullTransactionId ReadNextFullTransactionId(void) {
    FullTransactionId fullXid;

    // Acquire shared lock for safe concurrent reading
    LWLockAcquire(XidGenLock, LW_SHARED);

    // Read the next transaction ID from shared memory
    fullXid = TransamVariables->nextXid;

    // Release the lock
    LWLockRelease(XidGenLock);

    return fullXid;
}
```

Key simplifications made:
- Added explanatory comments for each logical step
- The function was already quite simple, so minimal changes were needed
- Preserved the essential lock-acquire-read-release pattern
- Maintained the atomic read operation integrity