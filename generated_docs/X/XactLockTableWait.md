# XactLockTableWait

## Location
src/backend/storage/lmgr/lmgr.c: 657 - 732

## Overview
XactLockTableWait blocks the calling transaction until a specified transaction (or its topmost parent transaction) commits or aborts, providing essential synchronization for MVCC operations.

## Definition
```c
void XactLockTableWait(TransactionId xid, Relation rel, ItemPointer ctid, XLTW_Oper oper)
```

## Detailed Description
XactLockTableWait is a crucial synchronization primitive in PostgreSQL's MVCC system that allows one transaction to wait for another transaction to complete. The function works by attempting to acquire a ShareLock on the target transaction's XID - since the running transaction holds an ExclusiveLock on its own XID, the ShareLock request will block until the transaction completes.

The function handles subtransactions intelligently by automatically waiting on the topmost parent transaction when a subtransaction's lock has already been released. This ensures correct behavior even when dealing with complex nested transaction scenarios. If the initial XID belongs to a subtransaction that has already completed, the function traverses up the transaction hierarchy to find the topmost transaction that is still running.

The function also includes sophisticated error context handling, allowing it to provide detailed error messages about what operation was blocked and on which tuple, making debugging and monitoring easier.

## Parameters / Member Variables
- `xid`: The TransactionId to wait for completion
- `rel`: The relation involved in the operation (used for error context, can be NULL if oper is XLTW_None)
- `ctid`: The ItemPointer of the tuple involved (used for error context, can be NULL if oper is XLTW_None)  
- `oper`: The XLTW_Oper enumeration specifying the operation type for error reporting (use XLTW_None to disable error context)

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_TRANSACTION (constructs transaction-specific lock tag)
  - [LockAcquire](../L/LockAcquire.md)/LockRelease (acquires and releases ShareLock on transaction)
  - TransactionIdIsInProgress (checks if transaction is still running)
  - [SubTransGetTopmostTransaction](../S/SubTransGetTopmostTransaction.md) (finds the topmost parent transaction)
  - [GetTopTransactionIdIfAny](../G/GetTopTransactionIdIfAny.md) (gets current transaction's XID for comparison)
  - [XactLockTableWaitErrorCb](XactLockTableWaitErrorCb.md) (error context callback function)
  - [pg_usleep](../p/pg_usleep.md) (sleep function for retry logic)
- Called from (representative examples):
  - [heap_delete](../h/heap_delete.md), heap_update, heap_lock_tuple (heap access methods)
  - [_bt_doinsert](../b/_bt_doinsert.md) (B-tree index operations)
  - [check_exclusion_or_unique_constraint](../c/check_exclusion_or_unique_constraint.md) (constraint checking)
  - SnapBuildWaitSnapshot (logical replication)

## Notes and Other Information
- The function uses a ShareLock to wait, which is compatible with other ShareLocks but conflicts with the ExclusiveLock held by the running transaction
- Automatically handles subtransaction hierarchies by walking up to the topmost transaction
- Includes retry logic with pg_usleep(1000L) for cases where a transaction might be registered in ProcArray before the lock table
- Provides rich error context when oper != XLTW_None, including relation and tuple information
- Essential for preventing lost updates and ensuring proper MVCC semantics in concurrent environments
- The function will block indefinitely until the target transaction completes, making it unsuitable for cases where timeouts are needed (use ConditionalXactLockTableWait instead)
- Critical for maintaining data consistency in PostgreSQL's concurrent execution model