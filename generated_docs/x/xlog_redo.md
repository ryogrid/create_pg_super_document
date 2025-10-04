# xlog_redo

## Location
[src/backend/access/transam/xlog.c:8251-8608](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L8251-L8608)

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
  - [MultiXactSetNextMXact](../M/MultiXactSetNextMXact.md), MultiXactAdvanceOldest, MultiXactAdvanceNextMXact
  - [SetTransactionIdLimit](../S/SetTransactionIdLimit.md), TransactionIdPrecedes, TransactionIdRetreat
  - [GetCurrentReplayRecPtr](../G/GetCurrentReplayRecPtr.md), RecoveryRestartPoint
  - [PrescanPreparedTransactions](../P/PrescanPreparedTransactions.md), StandbyRecoverPreparedTransactions
  - [ProcArrayApplyRecoveryInfo](../P/ProcArrayApplyRecoveryInfo.md), InvalidateObsoleteReplicationSlots
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md), CommitTsParameterChange
  - [UpdateControlFile](../U/UpdateControlFile.md), CheckRequiredParameterValues
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

## Simplified Source

```c
void
xlog_redo(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecPtr lsn = record->EndRecPtr;

    if (info == XLOG_NEXTOID) {
        // Update next OID counter from WAL record
        Oid nextOid;
        memcpy(&nextOid, XLogRecGetData(record), sizeof(Oid));
        LWLockAcquire(OidGenLock, LW_EXCLUSIVE);
        TransamVariables->nextOid = nextOid;
        TransamVariables->oidCount = 0;
        LWLockRelease(OidGenLock);
    }
    else if (info == XLOG_CHECKPOINT_SHUTDOWN) {
        // Process shutdown checkpoint - trust counters exactly
        CheckPoint checkPoint;
        memcpy(&checkPoint, XLogRecGetData(record), sizeof(CheckPoint));

        // Update transaction and OID counters
        LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
        TransamVariables->nextXid = checkPoint.nextXid;
        LWLockRelease(XidGenLock);

        // Update multixact state and transaction limits
        MultiXactSetNextMXact(checkPoint.nextMulti, checkPoint.nextMultiOffset);
        SetTransactionIdLimit(checkPoint.oldestXid, checkPoint.oldestXidDB);

        // Handle standby recovery state for prepared transactions
        if (standbyState >= STANDBY_INITIALIZED) {
            // Create running transactions snapshot and apply recovery info
            // ... prepare transaction handling logic
        }

        // Update control file and validate timeline
        ControlFile->checkPointCopy = checkPoint;
        RecoveryRestartPoint(&checkPoint, record);
    }
    else if (info == XLOG_CHECKPOINT_ONLINE) {
        // Process online checkpoint - treat counters as minimums
        CheckPoint checkPoint;
        memcpy(&checkPoint, XLogRecGetData(record), sizeof(CheckPoint));

        // Update XID only if checkpoint is newer
        LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
        if (FullTransactionIdPrecedes(TransamVariables->nextXid, checkPoint.nextXid))
            TransamVariables->nextXid = checkPoint.nextXid;
        LWLockRelease(XidGenLock);

        // Update multixact and transaction limits
        MultiXactAdvanceNextMXact(checkPoint.nextMulti, checkPoint.nextMultiOffset);
        RecoveryRestartPoint(&checkPoint, record);
    }
    else if (info == XLOG_FPI || info == XLOG_FPI_FOR_HINT) {
        // Restore full-page images for crash consistency
        for (uint8 block_id = 0; block_id <= XLogRecMaxBlockId(record); block_id++) {
            Buffer buffer;
            if (XLogRecHasBlockImage(record, block_id)) {
                XLogReadBufferForRedo(record, block_id, &buffer);
                UnlockReleaseBuffer(buffer);
            }
        }
    }
    else if (info == XLOG_PARAMETER_CHANGE) {
        // Update configuration parameters from WAL
        xl_parameter_change xlrec;
        memcpy(&xlrec, XLogRecGetData(record), sizeof(xl_parameter_change));

        // Update control file with new parameters
        LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
        ControlFile->MaxConnections = xlrec.MaxConnections;
        ControlFile->wal_level = xlrec.wal_level;
        // ... other parameter updates
        UpdateControlFile();
        LWLockRelease(ControlFileLock);
    }
    // ... other record types handled with minimal processing
}
```