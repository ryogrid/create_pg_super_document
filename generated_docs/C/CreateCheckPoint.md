# CreateCheckPoint

## Location
[src/backend/access/transam/xlog.c:6863-7368](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L6863-L7368)

## Overview
Performs a comprehensive checkpoint operation that ensures data consistency by flushing all dirty buffers to disk, recording critical system state, and managing WAL records for both online and shutdown scenarios.

## Definition

```c
void CreateCheckPoint(int flags);
```

## Simplified Source

```c
// Simplified version of CreateCheckPoint
void CreateCheckPoint(int flags) {
    bool shutdown;
    CheckPoint checkPoint;
    XLogRecPtr recptr;
    XLogRecPtr last_important_lsn;
    VirtualTransactionId *vxids;
    int nvxids;

    // Determine checkpoint type: shutdown vs online
    shutdown = (flags & (CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_END_OF_RECOVERY));

    // Basic validation
    if (RecoveryInProgress() && !(flags & CHECKPOINT_END_OF_RECOVERY)) {
        elog(ERROR, "can't create a checkpoint during recovery");
    }

    // Initialize checkpoint statistics and prepare storage manager
    MemSet(&CheckpointStats, 0, sizeof(CheckpointStats));
    CheckpointStats.ckpt_start_t = GetCurrentTimestamp();
    SyncPreCheckpoint();

    START_CRIT_SECTION();

    // Update control file state for shutdown
    if (shutdown) {
        LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
        ControlFile->state = DB_SHUTDOWNING;
        UpdateControlFile();
        LWLockRelease(ControlFileLock);
    }

    // Initialize checkpoint record with current time and system state
    MemSet(&checkPoint, 0, sizeof(checkPoint));
    checkPoint.time = (pg_time_t) time(NULL);

    // Set transaction IDs for Hot Standby
    if (!shutdown && XLogStandbyInfoActive()) {
        checkPoint.oldestActiveXid = GetOldestActiveTransactionId();
    }

    // Check if checkpoint can be skipped (system idle)
    last_important_lsn = GetLastImportantRecPtr();
    if (!(flags & (CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_END_OF_RECOVERY | CHECKPOINT_FORCE))) {
        if (last_important_lsn == ControlFile->checkPoint) {
            END_CRIT_SECTION();
            return; // Skip checkpoint - system is idle
        }
    }

    // Set timeline information
    checkPoint.ThisTimeLineID = XLogCtl->InsertTimeLineID;
    checkPoint.PrevTimeLineID = (flags & CHECKPOINT_END_OF_RECOVERY) ?
                                XLogCtl->PrevTimeLineID : checkPoint.ThisTimeLineID;

    // Acquire WAL insertion locks and set checkpoint fields
    WALInsertLockAcquireExclusive();
    checkPoint.fullPageWrites = XLogCtl->Insert.fullPageWrites;
    checkPoint.wal_level = wal_level;

    // Handle redo pointer for shutdown vs online checkpoints
    if (shutdown) {
        // For shutdown: compute redo pointer directly
        XLogRecPtr curInsert = XLogBytePosToRecPtr(XLogCtl->Insert.CurrBytePos);
        checkPoint.redo = curInsert; // Simplified calculation
        RedoRecPtr = XLogCtl->Insert.RedoRecPtr = checkPoint.redo;
    }

    WALInsertLockRelease();

    // For online checkpoints: insert REDO record to mark redo point
    if (!shutdown) {
        XLogBeginInsert();
        XLogRegisterData((char *) &wal_level, sizeof(wal_level));
        XLogInsert(RM_XLOG_ID, XLOG_CHECKPOINT_REDO);
        checkPoint.redo = RedoRecPtr;
    }

    // Update shared memory redo pointer
    SpinLockAcquire(&XLogCtl->info_lck);
    XLogCtl->RedoRecPtr = checkPoint.redo;
    SpinLockRelease(&XLogCtl->info_lck);

    // Log checkpoint start and update process title
    if (log_checkpoints) {
        LogCheckpointStart(flags, false);
    }
    update_checkpoint_display(flags, false, false);

    // Gather transaction and OID information
    LWLockAcquire(XidGenLock, LW_SHARED);
    checkPoint.nextXid = TransamVariables->nextXid;
    checkPoint.oldestXid = TransamVariables->oldestXid;
    checkPoint.oldestXidDB = TransamVariables->oldestXidDB;
    LWLockRelease(XidGenLock);

    LWLockAcquire(OidGenLock, LW_SHARED);
    checkPoint.nextOid = TransamVariables->nextOid;
    if (!shutdown) {
        checkPoint.nextOid += TransamVariables->oidCount;
    }
    LWLockRelease(OidGenLock);

    // Get multixact information
    MultiXactGetCheckptMulti(shutdown, &checkPoint.nextMulti,
                            &checkPoint.nextMultiOffset,
                            &checkPoint.oldestMulti, &checkPoint.oldestMultiDB);

    END_CRIT_SECTION();

    // Wait for delayed transactions before starting checkpoint work
    vxids = GetVirtualXIDsDelayingChkpt(&nvxids, DELAY_CHKPT_START);
    if (nvxids > 0) {
        do {
            AbsorbSyncRequests();
            pg_usleep(10000L); // Wait 10ms
        } while (HaveVirtualXIDsDelayingChkpt(vxids, nvxids, DELAY_CHKPT_START));
    }
    pfree(vxids);

    // Perform the main checkpoint work (flush buffers, etc.)
    CheckPointGuts(checkPoint.redo, flags);

    // Wait for delayed transactions before completing checkpoint
    vxids = GetVirtualXIDsDelayingChkpt(&nvxids, DELAY_CHKPT_COMPLETE);
    if (nvxids > 0) {
        do {
            AbsorbSyncRequests();
            pg_usleep(10000L);
        } while (HaveVirtualXIDsDelayingChkpt(vxids, nvxids, DELAY_CHKPT_COMPLETE));
    }
    pfree(vxids);

    // Log standby snapshot for Hot Standby
    if (!shutdown && XLogStandbyInfoActive()) {
        LogStandbySnapshot();
    }

    START_CRIT_SECTION();

    // Insert the checkpoint record into WAL
    XLogBeginInsert();
    XLogRegisterData((char *) (&checkPoint), sizeof(checkPoint));
    recptr = XLogInsert(RM_XLOG_ID, shutdown ? XLOG_CHECKPOINT_SHUTDOWN :
                                              XLOG_CHECKPOINT_ONLINE);
    XLogFlush(recptr);

    // Update control file with new checkpoint information
    LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
    if (shutdown) {
        ControlFile->state = DB_SHUTDOWNED;
    }
    ControlFile->checkPoint = ProcLastRecPtr;
    ControlFile->checkPointCopy = checkPoint;
    ControlFile->minRecoveryPoint = InvalidXLogRecPtr;
    UpdateControlFile();
    LWLockRelease(ControlFileLock);

    // Update shared memory checkpoint XID
    SpinLockAcquire(&XLogCtl->info_lck);
    XLogCtl->ckptFullXid = checkPoint.nextXid;
    SpinLockRelease(&XLogCtl->info_lck);

    END_CRIT_SECTION();

    // Post-checkpoint cleanup
    SetWalSummarizerLatch();
    SyncPostCheckpoint();

    // Clean up old WAL files and manage replication slots
    XLogRecPtr slotsMinReqLSN = XLogGetReplicationSlotMinimumLSN();
    XLogSegNo _logSegNo;
    XLByteToSeg(RedoRecPtr, _logSegNo, wal_segment_size);
    KeepLogSeg(recptr, slotsMinReqLSN, &_logSegNo);

    // Remove obsolete WAL segments
    RemoveOldXlogFiles(_logSegNo - 1, RedoRecPtr, recptr, checkPoint.ThisTimeLineID);

    // Create new WAL files if needed (online checkpoints only)
    if (!shutdown) {
        PreallocXlogFiles(recptr, checkPoint.ThisTimeLineID);
    }

    // Cleanup pg_subtrans if not in recovery
    if (!RecoveryInProgress()) {
        TruncateSUBTRANS(GetOldestTransactionIdConsideredRunning());
    }

    // Log completion and update process title
    LogCheckpointEnd(false);
    update_checkpoint_display(flags, false, true);
}
```

Key simplifications made:
- Removed detailed freespace calculation for shutdown checkpoints
- Consolidated error handling and validation checks
- Abstracted complex WAL insertion lock coordination
- Simplified replication slot management and invalidation logic
- Removed detailed commit timestamp handling
- Focused on the main execution flow rather than edge cases
- Consolidated similar waiting loops for delayed transactions
- Abstracted low-level timeline and control file management details
## Detailed Description
CreateCheckPoint is the core function responsible for executing PostgreSQL's checkpoint mechanism, which is fundamental for data durability and crash recovery. The function handles two distinct checkpoint types: online checkpoints (during normal operation) and shutdown checkpoints (during database shutdown or end of recovery).

For online checkpoints, the function employs a two-phase WAL record approach: first inserting an XLOG_CHECKPOINT_REDO record to mark the redo point, then completing with an XLOG_CHECKPOINT_ONLINE record after all data is flushed. This allows concurrent WAL insertion during the potentially lengthy checkpoint process.

For shutdown checkpoints, only a single XLOG_CHECKPOINT_SHUTDOWN record is needed since no concurrent WAL activity is possible.

The function coordinates multiple subsystems including buffer management, transaction state, replication slots, and WAL segment management. It uses critical sections to ensure atomicity of critical operations and implements sophisticated waiting mechanisms to handle concurrent transactions that might delay checkpoint completion.

## Parameters / Member Variables
- `flags`: Bitwise OR of checkpoint control flags including:
  - CHECKPOINT_IS_SHUTDOWN: Database shutdown checkpoint
  - CHECKPOINT_END_OF_RECOVERY: End-of-recovery checkpoint
  - CHECKPOINT_IMMEDIATE: Complete checkpoint ASAP, ignoring completion target
  - CHECKPOINT_FORCE: Force checkpoint even without recent activity
  - CHECKPOINT_FLUSH_ALL: Also flush unlogged table buffers

## Dependencies
- Functions called/Symbols referenced:
  - [CheckPointGuts](CheckPointGuts.md) (core checkpoint work)
  - [SyncPreCheckpoint](../S/SyncPreCheckpoint.md)/SyncPostCheckpoint (storage manager coordination)
  - [WALInsertLockAcquireExclusive](../W/WALInsertLockAcquireExclusive.md)/WALInsertLockRelease (WAL coordination)
  - [XLogInsert](../X/XLogInsert.md)/XLogFlush (WAL record management)
  - [UpdateControlFile](../U/UpdateControlFile.md) (control file updates)
  - [LogCheckpointStart](../L/LogCheckpointStart.md)/LogCheckpointEnd (checkpoint logging)
  - [RemoveOldXlogFiles](../R/RemoveOldXlogFiles.md)/PreallocXlogFiles (WAL file management)
  - [GetVirtualXIDsDelayingChkpt](../G/GetVirtualXIDsDelayingChkpt.md) (transaction coordination)
  - [update_checkpoint_display](../u/update_checkpoint_display.md) (process status updates)
- Called from (representative examples):
  - [CheckpointerMain](CheckpointerMain.md)
  - [ShutdownXLOG](../S/ShutdownXLOG.md)
  - [RequestCheckpoint](../R/RequestCheckpoint.md)

## Notes and Other Information
- May take many minutes to execute on busy systems due to extensive I/O operations
- Uses critical sections for atomic control file updates but exits them during I/O-intensive operations
- Implements sophisticated transaction delay handling to ensure consistency with concurrent commits
- Manages replication slot synchronization and WAL segment cleanup
- Updates various system catalogs and maintains checkpoint statistics
- Handles timeline management for point-in-time recovery scenarios
- Process status is updated throughout execution for monitoring purposes
- Skip logic prevents unnecessary checkpoints when system is idle