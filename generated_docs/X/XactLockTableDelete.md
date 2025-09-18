# XactLockTableDelete

## Location
src/backend/storage/lmgr/lmgr.c: 633 - 656

## Overview
XactLockTableDelete explicitly removes the exclusive lock on a transaction ID, primarily used for subtransaction IDs when they are committed or aborted.

## Definition
```c
void XactLockTableDelete(TransactionId xid)
```

## Detailed Description
XactLockTableDelete is the counterpart to XactLockTableInsert, responsible for explicitly releasing the exclusive lock on a transaction ID. Unlike main transaction locks which are automatically released when the transaction ends, this function is specifically designed for managing subtransaction locks that need explicit cleanup.

When a subtransaction commits or aborts, its lock entry in the transaction lock table needs to be explicitly removed to allow other transactions that might be waiting on this subtransaction ID to proceed. The function constructs a transaction-specific lock tag and calls LockRelease to remove the exclusive lock that was previously acquired by XactLockTableInsert.

This explicit lock management is crucial for proper subtransaction handling in PostgreSQL's nested transaction system, ensuring that resources are properly released and waiting transactions are promptly notified when subtransactions complete.

## Parameters / Member Variables
- `xid`: The TransactionId (XID) for which to remove the lock entry from the transaction lock table

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_TRANSACTION (macro to construct transaction-specific lock tag)
  - LockRelease (releases the exclusive lock on the transaction)
  - ExclusiveLock (lock mode constant matching the lock mode that was acquired)
- Called from (representative examples):
  - CommitSubTransaction (when a subtransaction commits)
  - Various subtransaction management functions

## Notes and Other Information
- This function is NOT used for main transaction IDs - those locks are released automatically at transaction end
- Primarily used for subtransaction ID management in PostgreSQL's nested transaction system
- The function explicitly releases the ExclusiveLock that was acquired by XactLockTableInsert
- Essential for proper cleanup of subtransaction resources and notification of waiting transactions
- Part of PostgreSQL's hierarchical transaction management that supports savepoints and nested transactions
- The explicit lock release ensures that other transactions waiting on the subtransaction can immediately detect its completion
- Helps prevent lock table bloat by removing unnecessary lock entries for completed subtransactions