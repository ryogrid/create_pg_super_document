# ProcArrayApplyRecoveryInfo

## Location
[src/backend/storage/ipc/procarray.c:1054-1317](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L1054-L1317)

## Overview
ProcArrayApplyRecoveryInfo initializes and maintains the standby server's transaction state during recovery by processing running transaction information from the primary server.

## Definition
```c
void ProcArrayApplyRecoveryInfo(RunningTransactions running)
```

## Detailed Description
This function is the core mechanism for establishing consistent transaction visibility on standby servers during PostgreSQL recovery. It processes RunningTransactions data received from the primary server to reconstruct the state of active transactions.

The function operates through three distinct states:
- **STANDBY_INITIALIZED**: Initial state where KnownAssignedXids can be populated
- **STANDBY_SNAPSHOT_PENDING**: Waiting state when snapshot information is incomplete due to overflow
- **STANDBY_SNAPSHOT_READY**: Final state where recovery snapshots are fully functional

Key operations include:
1. Removing stale transactions and locks based on oldestRunningXid
2. Advancing the nextXid counter to match primary server state
3. Populating KnownAssignedXids with active transaction IDs from the primary
4. Handling snapshot overflow scenarios where subxid information is incomplete
5. Extending SUBTRANS to maintain subtransaction parent-child relationships
6. Setting global tracking variables for snapshot evolution

The function handles edge cases such as duplicate XIDs from prepared transactions, transactions that completed between snapshot creation and application, and overflow conditions where not all subtransaction information is available.

## Parameters / Member Variables
- `running`: RunningTransactions structure containing snapshot data from the primary server, including:
  - `nextXid`: Next transaction ID to be assigned on primary
  - `oldestRunningXid`: Oldest transaction still running on primary
  - `latestCompletedXid`: Most recent completed transaction
  - `xcnt`: Count of top-level transactions in xids array
  - `subxcnt`: Count of subtransactions in xids array
  - `xids`: Array of active transaction IDs
  - `subxid_status`: Status indicating completeness of subxid information

## Dependencies
- Functions called/Symbols referenced:
  - [ExpireOldKnownAssignedTransactionIds](../E/ExpireOldKnownAssignedTransactionIds.md)
  - TransactionIdRetreat
  - [AdvanceNextFullTransactionIdPastXid](../A/AdvanceNextFullTransactionIdPastXid.md)
  - [StandbyReleaseOldLocks](../S/StandbyReleaseOldLocks.md)
  - [KnownAssignedXidsReset](../K/KnownAssignedXidsReset.md)
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - [TransactionIdDidCommit](../T/TransactionIdDidCommit.md)
  - [TransactionIdDidAbort](../T/TransactionIdDidAbort.md)
  - [KnownAssignedXidsAdd](../K/KnownAssignedXidsAdd.md)
  - [ExtendSUBTRANS](../E/ExtendSUBTRANS.md)
  - [MaintainLatestCompletedXidRecovery](../M/MaintainLatestCompletedXidRecovery.md)
- Called from:
  - [StartupXLOG](../S/StartupXLOG.md) (during recovery processing)
  - [xlog_redo](../x/xlog_redo.md) (during WAL record replay)
  - [standby_redo](../s/standby_redo.md) (during standby-specific record processing)

## Notes and Other Information
- Must be called with standbyState >= STANDBY_INITIALIZED
- Requires exclusive ProcArrayLock during KnownAssignedXids manipulation
- Handles the complex case where snapshot information may be incomplete due to subxid overflow
- Critical for Hot Standby functionality and read-only query processing on standby servers
- The function can be called multiple times and must handle reentrant scenarios safely
- Sorts transaction IDs logically before adding to KnownAssignedXids to maintain ordering invariants