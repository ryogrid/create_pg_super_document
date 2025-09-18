# AssignTransactionId

## Location
src/backend/access/transam/xact.c: 632 - 787

## Overview
Assigns a new permanent transaction ID to a given transaction state, ensuring parent transactions have XIDs assigned first and handling WAL logging requirements for subtransaction management.

## Definition
```c
static void AssignTransactionId(TransactionState s)
```

## Detailed Description
This is the core function responsible for assigning transaction IDs in PostgreSQL. It implements a comprehensive transaction ID assignment process that ensures proper ordering, WAL logging, and subtransaction hierarchy management. The function handles both top-level transactions and subtransactions with different logic paths.

Key responsibilities include:
1. **Parent XID Assignment**: Ensures parent transactions have XIDs before child transactions, maintaining the invariant that child XIDs are always greater than parent XIDs
2. **Parallel Operation Safety**: Prevents XID assignment during parallel operations where transaction synchronization could be problematic
3. **WAL Logging**: Manages WAL logging requirements for logical replication and standby servers
4. **Resource Management**: Handles transaction locks and resource ownership
5. **Subtransaction Tracking**: Maintains subtransaction hierarchy in pg_subtrans and shared memory

The function uses an iterative approach rather than deep recursion to assign XIDs to parent transactions, preventing stack overflow in deeply nested subtransaction hierarchies.

## Parameters / Member Variables
- `s`: TransactionState pointer to the transaction that needs an XID assignment

## Dependencies
- Functions called/Symbols referenced:
  - FullTransactionIdIsValid
  - IsInParallelMode, IsParallelWorker
  - GetNewTransactionId
  - [SubTransSetParent](../S/SubTransSetParent.md)
  - XidFromFullTransactionId
  - [RegisterPredicateLockingXid](../R/RegisterPredicateLockingXid.md)
  - [XactLockTableInsert](../X/XactLockTableInsert.md)
  - XLogLogicalInfoActive, XLogStandbyInfoActive
  - [XLogBeginInsert](../X/XLogBeginInsert.md), XLogRegisterData, XLogInsert
  - [GetTopTransactionId](../G/GetTopTransactionId.md)
- Called from (representative examples):
  - [GetTopTransactionId](../G/GetTopTransactionId.md)
  - [GetCurrentTransactionId](../G/GetCurrentTransactionId.md)
  - [GetTopFullTransactionId](../G/GetTopFullTransactionId.md)
  - [GetCurrentFullTransactionId](../G/GetCurrentFullTransactionId.md)
  - [AssignTransactionId](AssignTransactionId.md) (recursive)

## Notes and Other Information
- Static function - only accessible within the same source file
- Prevents stack overflow by using iterative parent XID assignment
- Enforces transaction ordering invariant (child XID > parent XID)
- Handles WAL logging for logical replication and hot standby requirements
- Critical for maintaining transaction visibility and MVCC consistency
- Located in src/backend/access/transam/xact.c:632-787
- Manages unreported XIDs for hot standby servers via XLOG_XACT_ASSIGNMENT records