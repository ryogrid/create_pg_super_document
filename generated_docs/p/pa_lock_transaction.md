# pa_lock_transaction

## Location
src/backend/replication/logical/applyparallelworker.c: 1573 - 1579

## Overview
Acquires a transaction-level lock for parallel apply operations in logical replication, ensuring proper coordination between parallel apply workers.

## Definition
```c
void pa_lock_transaction(TransactionId xid, LOCKMODE lockmode)
```

## Detailed Description
This function is a critical component of PostgreSQL's logical replication parallel apply worker system. It acquires a transaction-specific lock using the PARALLEL_APPLY_LOCK_XACT lock type. The function serves as a coordination mechanism between parallel apply workers to prevent conflicts when processing transactions. A key design consideration is that all callers must pass a remote transaction ID rather than a local transaction ID, since local transaction IDs are only assigned when applying the first change in the parallel worker, but the first change might be blocked by concurrent transactions in other parallel workers.

## Parameters / Member Variables
- `xid`: The remote transaction ID for which to acquire the lock (must be remote, not local transaction ID)
- `lockmode`: The lock mode to use when acquiring the transaction lock

## Dependencies
- Functions called/Symbols referenced:
  - LockApplyTransactionForSession
  - PARALLEL_APPLY_LOCK_XACT
- Called from (representative examples):
  - pa_wait_for_xact_finish
  - apply_handle_stream_start

## Notes and Other Information
- Uses PARALLEL_APPLY_LOCK_XACT in the locktag_field4 to distinguish it as a transaction lock
- Critical requirement: callers must pass remote transaction IDs, not local ones
- The local transaction ID assignment happens only after applying the first change, which may be blocked
- This design allows the leader to communicate and wait using transaction locks before local transaction IDs are available
- Part of the broader parallel apply locking mechanism described in the file header comments
- Works in conjunction with MyLogicalRepWorker->subid to identify the subscription context