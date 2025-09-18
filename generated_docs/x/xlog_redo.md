# xlog_redo

## Location
src/backend/access/transam/xlog.c: 8251 - 8608

## Overview
xlog_redo is the main WAL resource manager redo function that handles replay of various XLOG record types during recovery, including checkpoints, parameter changes, and full-page image records.

## Definition
```c
void xlog_redo(XLogReaderState *record)
```

## Detailed Description
xlog_redo serves as the primary redo function for the XLOG resource manager (RM_XLOG_ID), responsible for replaying various types of WAL records during crash recovery, archive recovery, and streaming replication. The function dispatches different record types to their appropriate handling logic, including checkpoint processing, parameter updates, OID counter management, and full-page image restoration.

Key responsibilities include:
- Processing shutdown and online checkpoints to restore system state
- Updating transaction ID counters, OID counters, and multixact state
- Handling parameter changes that affect standby server configuration
- Restoring full-page images for crash consistency
- Managing timeline validation and recovery restart points
- Invalidating replication slots when necessary due to parameter changes

The function operates during various recovery modes and ensures that the replaying server maintains consistency with the primary server's state at the time the WAL records were generated.

## Parameters / Member Variables
- `record`: XLogReaderState structure containing the WAL record being replayed, including record data, LSN, and associated metadata

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo, XLogRecGetData, XLogRecHasAnyBlockRefs
  - MultiXactSetNextMXact, MultiXactAdvanceOldest, MultiXactAdvanceNextMXact
  - SetTransactionIdLimit, TransactionIdPrecedes, TransactionIdRetreat
  - GetCurrentReplayRecPtr, RecoveryRestartPoint
  - PrescanPreparedTransactions, StandbyRecoverPreparedTransactions
  - ProcArrayApplyRecoveryInfo, InvalidateObsoleteReplicationSlots
  - XLogReadBufferForRedo, CommitTsParameterChange
  - UpdateControlFile, CheckRequiredParameterValues
  - Various record types: XLOG_NEXTOID, XLOG_CHECKPOINT_SHUTDOWN, XLOG_CHECKPOINT_ONLINE, XLOG_FPI, etc.
- Called from (representative examples):
  - WAL replay subsystem during recovery operations

## Notes and Other Information
- The function handles multiple record types through a large if-else chain based on the record's info field
- Checkpoint records (shutdown vs online) are handled differently - shutdown checkpoints trust counters exactly while online checkpoints treat them as minimums
- Full-page image records (XLOG_FPI, XLOG_FPI_FOR_HINT) restore complete page images for crash consistency
- Parameter change records update both pg_control and may invalidate replication slots if wal_level changes
- The function includes extensive timeline validation to ensure records are being replayed in the correct context
- Some record types like XLOG_RESTORE_POINT and XLOG_BACKUP_END are handled elsewhere (in xlogrecovery.c)
- Critical for maintaining data consistency across primary and standby servers in streaming replication scenarios
- Includes special handling for prepared transactions during shutdown checkpoint processing in hot standby mode