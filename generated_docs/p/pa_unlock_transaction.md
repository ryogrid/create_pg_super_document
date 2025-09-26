# pa_unlock_transaction

## Location
[src/backend/replication/logical/applyparallelworker.c:1580-1590](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/applyparallelworker.c#L1580-L1590)

## Overview
Releases a transaction-level lock for parallel apply operations in logical replication, allowing other parallel apply workers to proceed with related operations.

## Definition
```c
void pa_unlock_transaction(TransactionId xid, LOCKMODE lockmode)
```

## Detailed Description
This function is the counterpart to pa_lock_transaction in PostgreSQL's logical replication parallel apply worker system. It releases a transaction-specific lock that was previously acquired using the PARALLEL_APPLY_LOCK_XACT lock type. The function ensures proper coordination between parallel apply workers by releasing locks when transactions complete, abort, or reach certain milestones. Like its locking counterpart, it operates on remote transaction IDs rather than local ones to maintain consistency in the parallel apply coordination mechanism.

## Parameters / Member Variables
- `xid`: The remote transaction ID for which to release the lock (must match the ID used when acquiring the lock)
- `lockmode`: The lock mode that was used when acquiring the lock (must match the original lock mode)

## Dependencies
- Functions called/Symbols referenced:
  - [UnlockApplyTransactionForSession](../U/UnlockApplyTransactionForSession.md)
  - PARALLEL_APPLY_LOCK_XACT
- Called from (representative examples):
  - [pa_wait_for_xact_finish](pa_wait_for_xact_finish.md)
  - [pa_stream_abort](pa_stream_abort.md)
  - [apply_handle_stream_prepare](../a/apply_handle_stream_prepare.md)
  - [apply_handle_stream_commit](../a/apply_handle_stream_commit.md)

## Notes and Other Information
- Counterpart function to pa_lock_transaction for releasing transaction locks
- Uses PARALLEL_APPLY_LOCK_XACT to identify the specific lock type being released
- Must use the same remote transaction ID and lock mode that were used when acquiring the lock
- Called in various transaction completion scenarios including commit, prepare, and abort operations
- Essential for preventing deadlocks and ensuring proper resource cleanup in parallel apply operations
- Works with MyLogicalRepWorker->subid to maintain subscription context consistency
- Part of the critical path for transaction completion in parallel logical replication