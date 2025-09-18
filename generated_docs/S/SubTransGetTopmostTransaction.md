# SubTransGetTopmostTransaction

## Location
[src/backend/access/transam/subtrans.c:163-200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/subtrans.c#L163-L200)

## Overview
Traverses the nested transaction hierarchy to find and return the topmost (root) transaction ID for a given subtransaction, essential for transaction visibility and conflict detection.

## Definition
```c
TransactionId
SubTransGetTopmostTransaction(TransactionId xid)
```

## Detailed Description
SubTransGetTopmostTransaction walks up the subtransaction hierarchy by repeatedly calling SubTransGetParent until it reaches the root transaction that has no parent. This function is crucial for PostgreSQL's MVCC implementation, particularly for snapshot visibility checks and transaction conflict detection.

The function includes important safeguards: it stops traversal when reaching transactions older than TransactionXmin (which may have been truncated), validates the parent-child relationship ordering to prevent infinite loops due to data corruption, and ensures that parent transaction IDs always precede their children according to PostgreSQL's transaction ID allocation convention.

Due to SUBTRANS log truncation, the function may return an intermediate subtransaction instead of the true topmost parent if the real parent predates TransactionXmin. This behavior is acceptable for the function's primary use cases in snapshot processing and transaction state queries.

## Parameters / Member Variables
- `xid`: The subtransaction ID for which to find the topmost parent transaction

## Dependencies
- Functions called/Symbols referenced:
  - [TransactionIdFollowsOrEquals](../T/TransactionIdFollowsOrEquals.md) (validates transaction ID is not too old)
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md) (checks transaction ID ordering)
  - [SubTransGetParent](SubTransGetParent.md) (retrieves parent transaction ID)
- Called from (representative examples):
  - [HeapCheckForSerializableConflictOut](../H/HeapCheckForSerializableConflictOut.md) (serializable isolation conflict detection)
  - TransactionIdIsInProgress (checking if transaction is still running)
  - [XactLockTableWait](../X/XactLockTableWait.md) (transaction lock waiting)
  - [ConditionalXactLockTableWait](../C/ConditionalXactLockTableWait.md) (non-blocking transaction lock waiting)
  - [XidInMVCCSnapshot](../X/XidInMVCCSnapshot.md) (MVCC snapshot visibility checks)

## Notes and Other Information
- May return an intermediate subtransaction instead of the true root if the actual parent predates TransactionXmin
- Includes corruption detection by validating that parent XIDs always precede child XIDs
- Essential for MVCC snapshot processing and transaction visibility determination
- The function stops traversal at TransactionXmin boundary to avoid accessing truncated data
- Used extensively in PostgreSQL's concurrency control mechanisms
- Part of the critical path for transaction visibility and conflict resolution in multi-level nested transactions