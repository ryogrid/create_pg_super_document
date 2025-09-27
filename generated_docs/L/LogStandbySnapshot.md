# LogStandbySnapshot

## Location
[src/backend/storage/ipc/standby.c:1285-1344](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/standby.c#L1285-L1344)

## Overview
LogStandbySnapshot logs the current transaction snapshot state to WAL, enabling standby servers to reconstruct the correct recovery snapshot and supporting logical decoding operations.

## Definition

```c
XLogRecPtr
LogStandbySnapshot(void)
```
## Detailed Description
This function captures and logs a comprehensive snapshot of the current system state to WAL, including all running transactions and AccessExclusiveLocks. This information is crucial for hot standby servers to maintain consistent recovery snapshots and for logical decoding to understand transaction visibility.

The function operates in two main phases: first, it captures all current AccessExclusiveLocks using GetRunningTransactionLocks() and logs them via LogAccessExclusiveLocks(); second, it captures all running transaction information using GetRunningTransactionData() and logs it via LogCurrentRunningXacts(). The function handles different WAL levels appropriately, releasing ProcArrayLock earlier for hot standby (WAL_LEVEL_REPLICA) but holding it longer for logical decoding (WAL_LEVEL_LOGICAL) to ensure consistency.

The timing and locking strategy is carefully designed to handle race conditions between snapshot derivation and WAL logging. The function ensures that standbys can reconstruct an accurate view of transaction state at the point when the snapshot was taken, enabling smooth transition to STANDBY_SNAPSHOT_READY state.

## Parameters / Member Variables
- No parameters (void function)
- Returns: XLogRecPtr of the last inserted WAL record

## Dependencies
- Functions called/Symbols referenced:
  - XLogStandbyInfoActive (verify standby info logging is enabled)
  - [GetRunningTransactionLocks](../G/GetRunningTransactionLocks.md) (capture current AccessExclusiveLocks)
  - [LogAccessExclusiveLocks](LogAccessExclusiveLocks.md) (log lock information to WAL)
  - [pfree](../p/pfree.md) (free allocated lock data)
  - [GetRunningTransactionData](../G/GetRunningTransactionData.md) (capture running transaction snapshot)
  - [LWLockRelease](LWLockRelease.md) (release ProcArrayLock and XidGenLock)
  - [LogCurrentRunningXacts](LogCurrentRunningXacts.md) (log transaction information to WAL)
- Data structures used:
  - RunningTransactions (transaction snapshot data)
  - [xl_standby_lock](../x/xl_standby_lock.md) (lock record format)
  - XLogRecPtr (WAL record pointer)
- Called from (representative examples):
  - [CreateCheckPoint](../C/CreateCheckPoint.md) (src/backend/access/transam/xlog.c:7189)
  - [pg_log_standby_snapshot](../p/pg_log_standby_snapshot.md) (src/backend/access/transam/xlogfuncs.c:217)
  - [BackgroundWriterMain](../B/BackgroundWriterMain.md) (src/backend/postmaster/bgwriter.c:290)
  - [SnapBuildWaitSnapshot](../S/SnapBuildWaitSnapshot.md) (src/backend/replication/logical/snapbuild.c:1603)
  - [ReplicationSlotReserveWal](../R/ReplicationSlotReserveWal.md) (src/backend/replication/slot.c:1466)

## Notes and Other Information
- Essential for hot standby functionality and logical decoding
- Handles different locking strategies based on WAL level (replica vs logical)
- Carefully manages race conditions during snapshot capture and WAL logging
- The function must be called with appropriate locks held (handled internally)
- Logs AccessExclusiveLocks first, then running transactions as the final record
- Standby servers use this information to transition to STANDBY_SNAPSHOT_READY state
- Contains extensive documentation about the hot standby recovery process and timing considerations
- Located in src/backend/storage/ipc/standby.c:1285-1344

## Simplified Source

```c
// Simplified version of LogStandbySnapshot
XLogRecPtr LogStandbySnapshot(void) {
    XLogRecPtr recptr;
    RunningTransactions running;
    xl_standby_lock *locks;
    int nlocks;

    // Verify standby logging is active
    Assert(XLogStandbyInfoActive());

    // Phase 1: Log current AccessExclusiveLocks
    locks = GetRunningTransactionLocks(&nlocks);
    if (nlocks > 0) {
        LogAccessExclusiveLocks(nlocks, locks);
    }
    pfree(locks);

    // Phase 2: Get snapshot of all running transactions
    // This must be the last record written for standby to open
    running = GetRunningTransactionData();

    // Handle lock release timing based on WAL level
    // For hot standby: release early (clog rechecks commit status)
    // For logical decoding: hold longer (avoid "future" clog issues)
    if (wal_level < WAL_LEVEL_LOGICAL) {
        LWLockRelease(ProcArrayLock);
    }

    // Log the running transaction snapshot
    recptr = LogCurrentRunningXacts(running);

    // Release remaining locks
    if (wal_level >= WAL_LEVEL_LOGICAL) {
        LWLockRelease(ProcArrayLock);
    }
    LWLockRelease(XidGenLock);

    return recptr;
}
```

Key simplifications made:
- Removed extensive comments about hot standby implementation details
- Consolidated lock release logic with clear explanations
- Focused on the two main phases: lock logging and transaction snapshot logging
- Preserved the critical locking strategy differences for different WAL levels
- Abstracted the complex race condition handling described in comments