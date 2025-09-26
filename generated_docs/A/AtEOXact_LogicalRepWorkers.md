# AtEOXact_LogicalRepWorkers

## Location
[src/backend/replication/logical/worker.c:5079-5110](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L5079-L5110)

## Overview
This function wakes up logical replication workers for subscriptions that were changed during the current transaction, but only if the transaction commits successfully.

## Definition
void AtEOXact_LogicalRepWorkers(bool isCommit)

## Detailed Description
AtEOXact_LogicalRepWorkers is called at the end of a transaction (EOXact = End of Transaction) to handle logical replication worker management. When a transaction that modified subscription configurations commits, this function ensures that all affected logical replication workers are notified to wake up and process the changes. The function operates on a global list of subscription IDs (on_commit_wakeup_workers_subids) that were marked for worker wakeup during the transaction. It only performs the wakeup operation if the transaction commits successfully; if the transaction is aborted, the worker wakeup list is simply cleared without any action.

## Parameters / Member Variables
- `isCommit`: A boolean flag indicating whether the current transaction is committing (true) or aborting (false)

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md) (with LogicalRepWorkerLock and LW_SHARED)
  - [logicalrep_workers_find](../l/logicalrep_workers_find.md)
  - [logicalrep_worker_wakeup_ptr](../l/logicalrep_worker_wakeup_ptr.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - lfirst_oid
  - lfirst
- Called from (representative examples):
  - [CommitTransaction](../C/CommitTransaction.md) (src/backend/access/transam/xact.c:2423)
  - [PrepareTransaction](../P/PrepareTransaction.md) (src/backend/access/transam/xact.c:2713)
  - [AbortTransaction](AbortTransaction.md) (src/backend/access/transam/xact.c:2931)

## Notes and Other Information
- The function uses shared locking (LW_SHARED) on LogicalRepWorkerLock to safely iterate through worker information
- The on_commit_wakeup_workers_subids list is automatically reclaimed during transaction cleanup
- This mechanism ensures that subscription changes only take effect when the transaction successfully commits
- Workers are found and awakened for each subscription ID that was modified during the transaction
- The function is part of PostgreSQL's logical replication infrastructure for maintaining consistency between publisher and subscriber databases