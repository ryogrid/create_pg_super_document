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

## Simplified Source

```c
// Simplified version of ProcArrayApplyRecoveryInfo
void ProcArrayApplyRecoveryInfo(RunningTransactions running) {
    TransactionId *xids;
    TransactionId advanceNextXid;
    int nxids;

    // Basic validation
    Assert(standbyState >= STANDBY_INITIALIZED);
    Assert(TransactionIdIsValid(running->nextXid));

    // Clean up stale transactions and locks
    ExpireOldKnownAssignedTransactionIds(running->oldestRunningXid);

    // Update nextXid to match primary
    advanceNextXid = running->nextXid;
    TransactionIdRetreat(advanceNextXid);
    AdvanceNextFullTransactionIdPastXid(advanceNextXid);

    StandbyReleaseOldLocks(running->oldestRunningXid);

    // If snapshot is already ready, nothing more to do
    if (standbyState == STANDBY_SNAPSHOT_READY)
        return;

    // Handle pending snapshot state
    if (standbyState == STANDBY_SNAPSHOT_PENDING) {
        // Check if we can reset pending state
        if (running->subxid_status != SUBXIDS_MISSING || running->xcnt == 0) {
            KnownAssignedXidsReset();
            standbyState = STANDBY_INITIALIZED;
        } else {
            // Check if pending condition is resolved
            if (TransactionIdPrecedes(standbySnapshotPendingXmin, running->oldestRunningXid)) {
                standbyState = STANDBY_SNAPSHOT_READY;
                elog(DEBUG1, "recovery snapshots are now enabled");
            }
            return;
        }
    }

    Assert(standbyState == STANDBY_INITIALIZED);

    LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

    // Build list of active XIDs
    xids = palloc(sizeof(TransactionId) * (running->xcnt + running->subxcnt));
    nxids = 0;

    for (int i = 0; i < running->xcnt + running->subxcnt; i++) {
        TransactionId xid = running->xids[i];

        // Skip already completed transactions
        if (TransactionIdDidCommit(xid) || TransactionIdDidAbort(xid))
            continue;

        xids[nxids++] = xid;
    }

    // Add XIDs to KnownAssignedXids if we have any
    if (nxids > 0) {
        if (procArray->numKnownAssignedXids != 0) {
            LWLockRelease(ProcArrayLock);
            elog(ERROR, "KnownAssignedXids is not empty");
        }

        // Sort XIDs before adding
        qsort(xids, nxids, sizeof(TransactionId), xidLogicalComparator);

        // Add sorted XIDs, skipping duplicates
        for (int i = 0; i < nxids; i++) {
            if (i > 0 && TransactionIdEquals(xids[i - 1], xids[i])) {
                elog(DEBUG1, "found duplicated transaction %u", xids[i]);
                continue;
            }
            KnownAssignedXidsAdd(xids[i], xids[i], true);
        }
    }

    pfree(xids);

    // Extend SUBTRANS up to nextXid
    TransactionIdAdvance(latestObservedXid);
    while (TransactionIdPrecedes(latestObservedXid, running->nextXid)) {
        ExtendSUBTRANS(latestObservedXid);
        TransactionIdAdvance(latestObservedXid);
    }
    TransactionIdRetreat(latestObservedXid);

    // Set final state based on subxid completeness
    if (running->subxid_status == SUBXIDS_MISSING) {
        standbyState = STANDBY_SNAPSHOT_PENDING;
        standbySnapshotPendingXmin = latestObservedXid;
        procArray->lastOverflowedXid = latestObservedXid;
    } else {
        standbyState = STANDBY_SNAPSHOT_READY;
        standbySnapshotPendingXmin = InvalidTransactionId;

        if (running->subxid_status == SUBXIDS_IN_SUBTRANS)
            procArray->lastOverflowedXid = latestObservedXid;
        else
            procArray->lastOverflowedXid = InvalidTransactionId;
    }

    // Update latest completed XID
    MaintainLatestCompletedXidRecovery(running->latestCompletedXid);

    LWLockRelease(ProcArrayLock);

    // Log final state
    if (standbyState == STANDBY_SNAPSHOT_READY)
        elog(DEBUG1, "recovery snapshots are now enabled");
    else
        elog(DEBUG1, "recovery snapshot waiting for complete snapshot");
}
```

Key simplifications made:
- Consolidated variable declarations and removed unnecessary temporary variables
- Simplified the state transition logic while preserving all three states
- Streamlined the XID collection and validation loop
- Abstracted complex debug logging into simpler messages
- Focused on the core algorithm: cleanup, advance XIDs, populate known assigned XIDs, set state
- Preserved essential locking, memory management, and error handling
- Maintained the overflow handling and SUBTRANS extension logic