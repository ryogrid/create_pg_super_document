# LogStandbySnapshot

## Location
src/backend/storage/ipc/standby.c: 1285 - 1344

## Overview
LogStandbySnapshot logs the current transaction snapshot state to WAL, enabling standby servers to reconstruct the correct recovery snapshot and supporting logical decoding operations.

## Definition


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
  - GetRunningTransactionLocks (capture current AccessExclusiveLocks)
  - LogAccessExclusiveLocks (log lock information to WAL)
  - pfree (free allocated lock data)
  - GetRunningTransactionData (capture running transaction snapshot)
  - LWLockRelease (release ProcArrayLock and XidGenLock)
  - LogCurrentRunningXacts (log transaction information to WAL)
- Data structures used:
  - RunningTransactions (transaction snapshot data)
  - xl_standby_lock (lock record format)
  - XLogRecPtr (WAL record pointer)
- Called from (representative examples):
  - CreateCheckPoint (src/backend/access/transam/xlog.c:7189)
  - pg_log_standby_snapshot (src/backend/access/transam/xlogfuncs.c:217)
  - BackgroundWriterMain (src/backend/postmaster/bgwriter.c:290)
  - SnapBuildWaitSnapshot (src/backend/replication/logical/snapbuild.c:1603)
  - ReplicationSlotReserveWal (src/backend/replication/slot.c:1466)

## Notes and Other Information
- Essential for hot standby functionality and logical decoding
- Handles different locking strategies based on WAL level (replica vs logical)
- Carefully manages race conditions during snapshot capture and WAL logging
- The function must be called with appropriate locks held (handled internally)
- Logs AccessExclusiveLocks first, then running transactions as the final record
- Standby servers use this information to transition to STANDBY_SNAPSHOT_READY state
- Contains extensive documentation about the hot standby recovery process and timing considerations
- Located in src/backend/storage/ipc/standby.c:1285-1344