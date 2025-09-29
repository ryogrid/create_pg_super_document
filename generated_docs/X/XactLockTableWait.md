# XactLockTableWait

## Location
[src/backend/storage/lmgr/lmgr.c:657-732](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L657-L732)

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
  - [TransactionIdIsInProgress](../T/TransactionIdIsInProgress.md) (checks if transaction is still running)
  - [SubTransGetTopmostTransaction](../S/SubTransGetTopmostTransaction.md) (finds the topmost parent transaction)
  - [GetTopTransactionIdIfAny](../G/GetTopTransactionIdIfAny.md) (gets current transaction's XID for comparison)
  - [XactLockTableWaitErrorCb](XactLockTableWaitErrorCb.md) (error context callback function)
  - [pg_usleep](../p/pg_usleep.md) (sleep function for retry logic)
- Called from (representative examples):
  - [heap_delete](../h/heap_delete.md), heap_update, heap_lock_tuple (heap access methods)
  - [_bt_doinsert](../b/_bt_doinsert.md) (B-tree index operations)
  - [check_exclusion_or_unique_constraint](../c/check_exclusion_or_unique_constraint.md) (constraint checking)
  - [SnapBuildWaitSnapshot](../S/SnapBuildWaitSnapshot.md) (logical replication)

## Notes and Other Information
- The function uses a ShareLock to wait, which is compatible with other ShareLocks but conflicts with the ExclusiveLock held by the running transaction
- Automatically handles subtransaction hierarchies by walking up to the topmost transaction
- Includes retry logic with pg_usleep(1000L) for cases where a transaction might be registered in ProcArray before the lock table
- Provides rich error context when oper != XLTW_None, including relation and tuple information
- Essential for preventing lost updates and ensuring proper MVCC semantics in concurrent environments
- The function will block indefinitely until the target transaction completes, making it unsuitable for cases where timeouts are needed (use ConditionalXactLockTableWait instead)
- Critical for maintaining data consistency in PostgreSQL's concurrent execution model

## Simplified Source

```c
// Simplified version of XactLockTableWait
void XactLockTableWait(TransactionId xid, Relation rel, ItemPointer ctid, XLTW_Oper oper) {
    LOCKTAG tag;
    XactLockTableWaitInfo info;
    ErrorContextCallback callback;
    bool first = true;

    // Set up error context callback if operation is specified
    if (oper != XLTW_None) {
        info.rel = rel;
        info.ctid = ctid;
        info.oper = oper;

        callback.callback = XactLockTableWaitErrorCb;
        callback.arg = &info;
        callback.previous = error_context_stack;
        error_context_stack = &callback;
    }

    // Main waiting loop
    for (;;) {
        // Create lock tag for the transaction
        SET_LOCKTAG_TRANSACTION(tag, xid);

        // Try to acquire ShareLock (blocks if transaction is still running)
        LockAcquire(&tag, ShareLock, false, false);

        // Immediately release the lock
        LockRelease(&tag, ShareLock, false);

        // Check if transaction has finished
        if (!TransactionIdIsInProgress(xid))
            break;

        // Handle subtransaction case: wait for topmost parent instead
        // Add small delay on retry to handle race conditions
        if (!first) {
            CHECK_FOR_INTERRUPTS();
            pg_usleep(1000L);
        }
        first = false;

        // Move up to topmost transaction in hierarchy
        xid = SubTransGetTopmostTransaction(xid);
    }

    // Restore error context stack
    if (oper != XLTW_None)
        error_context_stack = callback.previous;
}
```

Key simplifications made:
- Removed detailed comments explaining subtransaction behavior for cleaner flow
- Consolidated assertions and validation checks
- Simplified variable declarations and initialization
- Added high-level comments explaining the main logic steps
- Preserved essential algorithm: lock acquisition, progress checking, and subtransaction handling
- Maintained error context setup/teardown logic which is critical for debugging