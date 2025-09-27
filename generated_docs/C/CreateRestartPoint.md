# CreateRestartPoint

## Location
[src/backend/access/transam/xlog.c:7585-7880](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L7585-L7880)

## Overview
CreateRestartPoint establishes a restart point during WAL recovery, similar to CreateCheckPoint but used to create recovery checkpoints that allow rolling forward without replaying the entire recovery log.

## Definition
```c
bool CreateRestartPoint(int flags)
```

## Detailed Description
This function creates a restart point during recovery, which serves as a recovery checkpoint that enables faster recovery by establishing a point from which recovery can continue without replaying all WAL records from the beginning. The function performs extensive validation, updates control files, manages WAL segments, and handles replication slot synchronization.

The process includes:
1. Validating that a new restart point can be created based on the last safe checkpoint
2. Updating shared memory structures (RedoRecPtr) and control files
3. Performing checkpoint operations via CheckPointGuts()
4. Managing WAL segment cleanup and preallocation
5. Handling replication slot invalidation and synchronization
6. Truncating pg_subtrans when appropriate

## Parameters / Member Variables
- `flags`: Bitmap of checkpoint flags including CHECKPOINT_IS_SHUTDOWN to indicate shutdown restart points

## Dependencies
- Functions called/Symbols referenced:
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - [UpdateMinRecoveryPoint](../U/UpdateMinRecoveryPoint.md)
  - [CheckPointGuts](CheckPointGuts.md)
  - [KeepLogSeg](../K/KeepLogSeg.md)
  - [GetWalRcvFlushRecPtr](../G/GetWalRcvFlushRecPtr.md)
  - [GetXLogReplayRecPtr](../G/GetXLogReplayRecPtr.md)
  - [InvalidateObsoleteReplicationSlots](../I/InvalidateObsoleteReplicationSlots.md)
  - [RemoveOldXlogFiles](../R/RemoveOldXlogFiles.md)
  - [PreallocXlogFiles](../P/PreallocXlogFiles.md)
  - [TruncateSUBTRANS](../T/TruncateSUBTRANS.md)
  - [ExecuteRecoveryCommand](../E/ExecuteRecoveryCommand.md)
- Called from (representative examples):
  - [CheckpointerMain](CheckpointerMain.md) (in checkpointer process)
  - [ShutdownXLOG](../S/ShutdownXLOG.md) (during shutdown)

## Notes and Other Information
- This function must be called by the checkpointer process (B_CHECKPOINTER)
- Returns true if a new restart point was established, false otherwise
- Uses various locks including ControlFileLock and WALInsertLock for thread safety
- Integrates with archive_cleanup_command for external cleanup operations
- The function coordinates with replication slots to ensure WAL segments needed by slots are not removed
- Statistics are collected and logged similar to regular checkpoints
- Hot standby compatibility is maintained through proper minRecoveryPoint handling

## Simplified Source

```c
// Simplified version of CreateRestartPoint
bool CreateRestartPoint(int flags) {
    XLogRecPtr lastCheckPointRecPtr;
    XLogRecPtr lastCheckPointEndPtr;
    CheckPoint lastCheckPoint;
    XLogRecPtr PriorRedoPtr;
    XLogRecPtr receivePtr, replayPtr, endptr;
    TimeLineID replayTLI;
    XLogSegNo _logSegNo;
    XLogRecPtr slotsMinReqLSN;

    // Step 1: Get the last safe checkpoint record
    SpinLockAcquire(&XLogCtl->info_lck);
    lastCheckPointRecPtr = XLogCtl->lastCheckPointRecPtr;
    lastCheckPointEndPtr = XLogCtl->lastCheckPointEndPtr;
    lastCheckPoint = XLogCtl->lastCheckPoint;
    SpinLockRelease(&XLogCtl->info_lck);

    // Step 2: Verify we're still in recovery mode
    if (!RecoveryInProgress()) {
        ereport(DEBUG2, (errmsg_internal("skipping restartpoint, recovery has already ended")));
        return false;
    }

    // Step 3: Check if we can create a new restart point
    if (XLogRecPtrIsInvalid(lastCheckPointRecPtr) ||
        lastCheckPoint.redo <= ControlFile->checkPointCopy.redo) {

        // Update minimum recovery point and handle shutdown if needed
        UpdateMinRecoveryPoint(InvalidXLogRecPtr, true);
        if (flags & CHECKPOINT_IS_SHUTDOWN) {
            LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
            ControlFile->state = DB_SHUTDOWNED_IN_RECOVERY;
            UpdateControlFile();
            LWLockRelease(ControlFileLock);
        }
        return false;
    }

    // Step 4: Update shared RedoRecPtr for restart point tracking
    WALInsertLockAcquireExclusive();
    RedoRecPtr = XLogCtl->Insert.RedoRecPtr = lastCheckPoint.redo;
    WALInsertLockRelease();

    SpinLockAcquire(&XLogCtl->info_lck);
    XLogCtl->RedoRecPtr = lastCheckPoint.redo;
    SpinLockRelease(&XLogCtl->info_lck);

    // Step 5: Initialize checkpoint statistics and logging
    MemSet(&CheckpointStats, 0, sizeof(CheckpointStats));
    CheckpointStats.ckpt_start_t = GetCurrentTimestamp();

    slotsMinReqLSN = XLogGetReplicationSlotMinimumLSN();

    if (log_checkpoints)
        LogCheckpointStart(flags, true);

    update_checkpoint_display(flags, true, false);

    // Step 6: Perform the actual checkpoint work
    CheckPointGuts(lastCheckPoint.redo, flags);

    // Step 7: Update pg_control with new checkpoint information
    PriorRedoPtr = ControlFile->checkPointCopy.redo;

    LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
    if (ControlFile->checkPointCopy.redo < lastCheckPoint.redo) {
        ControlFile->checkPoint = lastCheckPointRecPtr;
        ControlFile->checkPointCopy = lastCheckPoint;

        // Update minimum recovery point for archive recovery
        if (ControlFile->state == DB_IN_ARCHIVE_RECOVERY) {
            if (ControlFile->minRecoveryPoint < lastCheckPointEndPtr) {
                ControlFile->minRecoveryPoint = lastCheckPointEndPtr;
                ControlFile->minRecoveryPointTLI = lastCheckPoint.ThisTimeLineID;
                LocalMinRecoveryPoint = ControlFile->minRecoveryPoint;
                LocalMinRecoveryPointTLI = ControlFile->minRecoveryPointTLI;
            }
            if (flags & CHECKPOINT_IS_SHUTDOWN)
                ControlFile->state = DB_SHUTDOWNED_IN_RECOVERY;
        }
        UpdateControlFile();
    }
    LWLockRelease(ControlFileLock);

    // Step 8: Update checkpoint distance statistics
    if (PriorRedoPtr != InvalidXLogRecPtr)
        UpdateCheckPointDistanceEstimate(RedoRecPtr - PriorRedoPtr);

    // Step 9: Clean up old WAL segments
    XLByteToSeg(RedoRecPtr, _logSegNo, wal_segment_size);

    receivePtr = GetWalRcvFlushRecPtr(NULL, NULL);
    replayPtr = GetXLogReplayRecPtr(&replayTLI);
    endptr = (receivePtr < replayPtr) ? replayPtr : receivePtr;

    KeepLogSeg(endptr, slotsMinReqLSN, &_logSegNo);

    // Handle replication slot invalidation if needed
    if (InvalidateObsoleteReplicationSlots(RS_INVAL_WAL_REMOVED, _logSegNo, InvalidOid, InvalidTransactionId)) {
        slotsMinReqLSN = XLogGetReplicationSlotMinimumLSN();
        CheckPointReplicationSlots(flags & CHECKPOINT_IS_SHUTDOWN);
        XLByteToSeg(RedoRecPtr, _logSegNo, wal_segment_size);
        KeepLogSeg(endptr, slotsMinReqLSN, &_logSegNo);
    }
    _logSegNo--;

    // Step 10: Remove old WAL files and preallocate new ones
    if (!RecoveryInProgress())
        replayTLI = XLogCtl->InsertTimeLineID;

    RemoveOldXlogFiles(_logSegNo, RedoRecPtr, endptr, replayTLI);
    PreallocXlogFiles(endptr, replayTLI);

    // Step 11: Truncate pg_subtrans if hot standby is enabled
    if (EnableHotStandby)
        TruncateSUBTRANS(GetOldestTransactionIdConsideredRunning());

    // Step 12: Complete checkpoint and log results
    LogCheckpointEnd(true);
    update_checkpoint_display(flags, true, true);

    TimestampTz xtime = GetLatestXTime();
    ereport((log_checkpoints ? LOG : DEBUG2),
            (errmsg("recovery restart point at %X/%X", LSN_FORMAT_ARGS(lastCheckPoint.redo)),
             xtime ? errdetail("Last completed transaction was at log time %s.",
                              timestamptz_to_str(xtime)) : 0));

    // Step 13: Execute cleanup command if configured
    if (archiveCleanupCommand && strcmp(archiveCleanupCommand, "") != 0)
        ExecuteRecoveryCommand(archiveCleanupCommand, "archive_cleanup_command",
                              false, WAIT_EVENT_ARCHIVE_CLEANUP_COMMAND);

    return true;
}
```

Key simplifications made:
- Removed detailed error handling comments for clarity
- Consolidated variable declarations at the top
- Added step-by-step comments to show the main execution flow
- Simplified conditional logic where possible
- Focused on the main execution path
- Abstracted low-level locking details with clear purpose comments
- Maintained all essential functionality and correctness