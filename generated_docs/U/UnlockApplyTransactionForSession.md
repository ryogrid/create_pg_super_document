# UnlockApplyTransactionForSession

## Location
src/backend/storage/lmgr/lmgr.c: 1217 - 1238

## Overview
Releases a lock on a specific apply transaction for a logical replication subscription, allowing other processes to access the transaction.

## Definition

```c
void
UnlockApplyTransactionForSession(Oid suboid, TransactionId xid, uint16 objid,
								 LOCKMODE lockmode)
```
## Detailed Description
This function is part of PostgreSQL's logical replication locking mechanism. It releases a lock that was previously acquired on an apply transaction for a specific subscription. The function constructs an apply transaction lock tag using the provided subscription ID, transaction ID, and object ID, then releases the lock using the specified lock mode. This is typically called when a parallel apply worker has finished processing a transaction and needs to release its exclusive access to that transaction.

## Parameters / Member Variables
- : Object ID of the logical replication subscription
- : Transaction ID of the apply transaction being unlocked
- : Object identifier within the transaction context (typically used for parallel worker identification)
- : The lock mode to release (should match the mode used when acquiring the lock)

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_APPLY_TRANSACTION (macro to construct the lock tag)
  - LockRelease (releases the actual lock)
- Called from (representative examples):
  - pa_unlock_stream (parallel apply worker stream unlocking)
  - pa_unlock_transaction (parallel apply worker transaction unlocking)

## Notes and Other Information
- This function is specifically designed for logical replication's parallel apply workers
- The lock tag is constructed using MyDatabaseId, ensuring database-specific locking
- The  parameter passed to LockRelease indicates this is a session-level lock
- Part of the apply transaction locking infrastructure that prevents conflicts between parallel workers processing the same subscription