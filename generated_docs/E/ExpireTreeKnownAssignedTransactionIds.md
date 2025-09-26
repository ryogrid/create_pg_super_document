# ExpireTreeKnownAssignedTransactionIds

## Location
[src/backend/storage/ipc/procarray.c:4471-4496](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L4471-L4496)

## Overview
Removes a transaction and its subtransactions from the KnownAssignedXids data structure during recovery, analogous to ProcArrayEndTransaction() but for standby servers.

## Definition
```c
void ExpireTreeKnownAssignedTransactionIds(TransactionId xid, int nsubxids,
                                         TransactionId *subxids, TransactionId max_xid)
```

## Detailed Description
This function is called during recovery on standby servers to remove completed transactions from the KnownAssignedXids tracking structure. It serves as the recovery-time equivalent of ProcArrayEndTransaction(), handling both the main transaction and any associated subtransactions. The function ensures proper synchronization using the same locking mechanism as regular transaction commits, maintains the latest completed transaction ID for recovery purposes, and updates the transaction completion counter.

## Parameters / Member Variables
- `xid`: The main transaction ID to be removed from KnownAssignedXids
- `nsubxids`: The number of subtransaction IDs in the subxids array
- `subxids`: Array of subtransaction IDs to be removed along with the main transaction
- `max_xid`: The maximum transaction ID seen, used to advance latestCompletedXid during recovery

## Dependencies
- Functions called/Symbols referenced:
  - STANDBY_INITIALIZED (constant for standby state checking)
  - LWLockAcquire (for ProcArrayLock exclusive access)
  - KnownAssignedXidsRemoveTree (removes the transaction tree from KnownAssignedXids)
  - MaintainLatestCompletedXidRecovery (advances latestCompletedXid)
  - LWLockRelease (releases ProcArrayLock)
- Called from (representative examples):
  - xact_redo_commit (during commit record replay in recovery)
  - xact_redo_abort (during abort record replay in recovery)

## Notes and Other Information
- This function can only be called when standbyState >= STANDBY_INITIALIZED
- Uses exclusive locking on ProcArrayLock to ensure thread safety, matching the locking pattern used in normal transaction commits
- Increments xactCompletionCount to maintain consistency with normal transaction processing
- The function is part of PostgreSQL's Hot Standby functionality, allowing read-only queries on standby servers during recovery