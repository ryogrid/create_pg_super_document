# RecordKnownAssignedTransactionIds

## Location
[src/backend/storage/ipc/procarray.c:4402-4470](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L4402-L4470)

## Overview
Records a transaction ID and any preceding unobserved transaction IDs in the KnownAssignedXids array during Hot Standby recovery, maintaining the list of transactions that are running on the primary server.

## Definition
```c
void RecordKnownAssignedTransactionIds(TransactionId xid)
```

## Detailed Description
This function is a critical component of PostgreSQL's Hot Standby functionality. It maintains the KnownAssignedXids array, which tracks transaction IDs that are (or were) running on the primary server at the current point in WAL replay. The function ensures that standby servers have accurate visibility information by recording not only the observed transaction ID but also any intervening transaction IDs that must have been assigned due to PostgreSQL's sequential XID assignment policy.

When a new transaction ID is observed during WAL replay, the function checks if it follows the latest observed XID. If there's a gap, it means intermediate XIDs were assigned on the primary but not yet observed in WAL records. The function fills this gap by adding all intermediate XIDs to the KnownAssignedXids array and extending the subtransaction tracking system (SUBTRANS) accordingly.

The function operates in different modes depending on the standby state. In early recovery phases, it only extends SUBTRANS and updates latestObservedXid. Once the KnownAssignedXids machinery is fully initialized, it also maintains the array of assigned transaction IDs that standby transactions must consider as running.

## Parameters / Member Variables
- `xid`: Transaction ID observed in a WAL record that needs to be recorded as assigned

## Dependencies
- Functions called/Symbols referenced:
  - [TransactionIdFollows](../T/TransactionIdFollows.md)
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - TransactionIdAdvance
  - [ExtendSUBTRANS](../E/ExtendSUBTRANS.md)
  - KnownAssignedXidsAdd
  - AdvanceNextFullTransactionIdPastXid
  - STANDBY_INITIALIZED (constant)
  - DEBUG4 (logging level)
- Called from (representative examples):
  - [xact_redo_commit](../x/xact_redo_commit.md)
  - [xact_redo_abort](../x/xact_redo_abort.md)
  - [ApplyWalRecord](../A/ApplyWalRecord.md)
  - ProcArrayApplyXidAssignment

## Notes and Other Information
- Must be called for every WAL record associated with a transaction during recovery
- Only operates after StartupCLOG() and related initialization is complete
- Fills gaps in transaction ID sequence by inferring unobserved XIDs
- Part of Hot Standby's mechanism to maintain accurate transaction visibility
- Extends SUBTRANS for all intermediate transaction IDs to prevent lookup failures
- Updates latestObservedXid and advances nextXid to maintain consistency
- Essential for ensuring standby transactions see the correct set of running transactions
- The function handles the sequential nature of PostgreSQL's transaction ID assignment