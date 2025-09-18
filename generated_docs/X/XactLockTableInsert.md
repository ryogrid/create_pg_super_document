# XactLockTableInsert

## Location
[src/backend/storage/lmgr/lmgr.c:616-632](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L616-L632)

## Overview
XactLockTableInsert acquires an exclusive lock on a transaction ID to indicate that the transaction is currently running, allowing other transactions to wait for its completion.

## Definition
```c
void XactLockTableInsert(TransactionId xid)
```

## Detailed Description
XactLockTableInsert is a critical function in PostgreSQL's transaction management system that creates a lock entry in the lock table for a given transaction ID (XID). When called, it acquires an exclusive lock on the transaction ID, which serves as a signal to other transactions that this XID is currently active and running.

This mechanism is fundamental to PostgreSQL's MVCC (Multi-Version Concurrency Control) implementation. The exclusive lock on the transaction ID allows other transactions to wait for the completion of this transaction using XactLockTableWait. The function is typically called when a transaction first acquires its XID, either through explicit assignment or when the transaction first performs a write operation.

The lock is held until the transaction commits or aborts, at which point it is automatically released by the lock manager. This provides a reliable way for other transactions to detect when a transaction has finished and its changes are either committed or rolled back.

## Parameters / Member Variables
- `xid`: The TransactionId (XID) for which to insert a lock entry in the transaction lock table

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_TRANSACTION (macro to construct transaction-specific lock tag)
  - [LockAcquire](../L/LockAcquire.md) (acquires the exclusive lock on the transaction)
  - ExclusiveLock (lock mode constant for exclusive access)
- Called from (representative examples):
  - [AssignTransactionId](../A/AssignTransactionId.md) (when a transaction first gets its XID)
  - Various transaction management functions

## Notes and Other Information
- The lock is acquired in ExclusiveLock mode, preventing any other transaction from acquiring a conflicting lock on the same XID
- This function is essential for transaction dependency tracking and deadlock detection
- The lock is automatically released when the transaction ends (commits or aborts)
- Part of PostgreSQL's sophisticated concurrency control mechanism that enables MVCC
- The function uses 'false, false' parameters for LockAcquire, meaning it will block if the lock cannot be immediately acquired and won't report the lock to the user
- This is one of the core primitives that enables PostgreSQL's ability to have multiple concurrent transactions while maintaining data consistency