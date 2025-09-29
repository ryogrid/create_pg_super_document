# ApplyWalRecord

## Location
[src/backend/access/transam/xlogrecovery.c:1908-2071](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L1908-L2071)

## Overview
ApplyWalRecord is a subroutine of PerformWalRecovery that applies a single WAL record during recovery, handling timeline switches, transaction ID advancement, and various recovery-specific operations.

## Definition

```c
static void
ApplyWalRecord(XLogReaderState *xlogreader, XLogRecord *record, TimeLineID *replayTLI)
```
## Detailed Description
ApplyWalRecord processes and applies a single WAL record during PostgreSQL recovery. The function performs several critical operations:

1. **Error Context Setup**: Establishes error handling callbacks for better error reporting during replay
2. **Transaction ID Management**: Advances the next transaction ID beyond the record's XID to maintain consistency
3. **Timeline Switch Handling**: Detects and processes timeline changes from checkpoint and end-of-recovery records
4. **Recovery State Updates**: Updates shared memory structures to track replay progress
5. **Hot Standby Processing**: Records known assigned transaction IDs when in Hot Standby mode
6. **Record Application**: Delegates actual record processing to appropriate resource managers
7. **Consistency Verification**: Performs backup page consistency checks when enabled
8. **Walsender Notification**: Wakes up physical and logical walsenders based on cascading replication settings
9. **Timeline Cleanup**: Removes obsolete WAL files when switching timelines

The function handles special XLOG records (checkpoints, end-of-recovery) differently and coordinates with various PostgreSQL subsystems during recovery.

## Parameters / Member Variables
- : XLogReaderState pointer containing the current WAL record and reading state
- : XLogRecord pointer to the specific WAL record being applied
- : TimeLineID pointer that tracks the current replay timeline and may be updated during timeline switches

## Dependencies
- Functions called/Symbols referenced:
  - [xlogrecovery_redo](../x/xlogrecovery_redo.md)
  - [AdvanceNextFullTransactionIdPastXid](AdvanceNextFullTransactionIdPastXid.md)
  - [RecordKnownAssignedTransactionIds](../R/RecordKnownAssignedTransactionIds.md)
  - [checkTimeLineSwitch](../c/checkTimeLineSwitch.md)
  - [verifyBackupPageConsistency](../v/verifyBackupPageConsistency.md)
  - [CheckRecoveryConsistency](../C/CheckRecoveryConsistency.md)
  - [RemoveNonParentXlogFiles](../R/RemoveNonParentXlogFiles.md)
  - [WalSndWakeup](../W/WalSndWakeup.md)
  - [WalRcvForceReply](../W/WalRcvForceReply.md)
  - [XLogPrefetchReconfigure](../X/XLogPrefetchReconfigure.md)
  - [GetRmgr](../G/GetRmgr.md)
- Called from:
  - [PerformWalRecovery](../P/PerformWalRecovery.md) (src/backend/access/transam/xlogrecovery.c:1822)

## Notes and Other Information
- This is a static function only called from within the xlogrecovery.c module
- The function maintains error context callbacks for detailed error reporting during recovery
- Timeline switches are detected by examining checkpoint and end-of-recovery records
- Hot Standby transaction tracking is performed when the system is in initialized standby state
- Walsender wakeup behavior differs between physical and logical replication
- The function coordinates with the WAL prefetcher to optimize I/O performance
- Backup page consistency checks are optional and controlled by the XLR_CHECK_CONSISTENCY flag
- Timeline switches trigger cleanup of potentially invalid future WAL segments

## Simplified Source

```c
// Simplified version of ApplyWalRecord
static void
ApplyWalRecord(XLogReaderState *xlogreader, XLogRecord *record, TimeLineID *replayTLI)
{
    ErrorContextCallback errcallback;
    bool switchedTLI = false;

    // Setup error traceback support
    errcallback.callback = rm_redo_error_callback;
    errcallback.arg = xlogreader;
    errcallback.previous = error_context_stack;
    error_context_stack = &errcallback;

    // Advance transaction ID beyond this record's XID
    AdvanceNextFullTransactionIdPastXid(record->xl_xid);

    // Handle timeline switches for checkpoint and end-of-recovery records
    if (record->xl_rmid == RM_XLOG_ID) {
        TimeLineID newReplayTLI = *replayTLI;
        TimeLineID prevReplayTLI = *replayTLI;
        uint8 info = record->xl_info & ~XLR_INFO_MASK;

        if (info == XLOG_CHECKPOINT_SHUTDOWN) {
            CheckPoint checkPoint;
            memcpy(&checkPoint, XLogRecGetData(xlogreader), sizeof(CheckPoint));
            newReplayTLI = checkPoint.ThisTimeLineID;
            prevReplayTLI = checkPoint.PrevTimeLineID;
        }
        else if (info == XLOG_END_OF_RECOVERY) {
            xl_end_of_recovery xlrec;
            memcpy(&xlrec, XLogRecGetData(xlogreader), sizeof(xl_end_of_recovery));
            newReplayTLI = xlrec.ThisTimeLineID;
            prevReplayTLI = xlrec.PrevTimeLineID;
        }

        // Switch timeline if needed
        if (newReplayTLI != *replayTLI) {
            checkTimeLineSwitch(xlogreader->EndRecPtr, newReplayTLI, prevReplayTLI, *replayTLI);
            *replayTLI = newReplayTLI;
            switchedTLI = true;
        }
    }

    // Update shared replay progress tracking
    SpinLockAcquire(&XLogRecoveryCtl->info_lck);
    XLogRecoveryCtl->replayEndRecPtr = xlogreader->EndRecPtr;
    XLogRecoveryCtl->replayEndTLI = *replayTLI;
    SpinLockRelease(&XLogRecoveryCtl->info_lck);

    // Record transaction IDs for Hot Standby mode
    if (standbyState >= STANDBY_INITIALIZED && TransactionIdIsValid(record->xl_xid)) {
        RecordKnownAssignedTransactionIds(record->xl_xid);
    }

    // Handle special XLOG records directly
    if (record->xl_rmid == RM_XLOG_ID) {
        xlogrecovery_redo(xlogreader, *replayTLI);
    }

    // Apply the WAL record using appropriate resource manager
    GetRmgr(record->xl_rmid).rm_redo(xlogreader);

    // Verify backup page consistency if enabled
    if ((record->xl_info & XLR_CHECK_CONSISTENCY) != 0) {
        verifyBackupPageConsistency(xlogreader);
    }

    // Restore error context
    error_context_stack = errcallback.previous;

    // Update final replay tracking
    SpinLockAcquire(&XLogRecoveryCtl->info_lck);
    XLogRecoveryCtl->lastReplayedReadRecPtr = xlogreader->ReadRecPtr;
    XLogRecoveryCtl->lastReplayedEndRecPtr = xlogreader->EndRecPtr;
    XLogRecoveryCtl->lastReplayedTLI = *replayTLI;
    SpinLockRelease(&XLogRecoveryCtl->info_lck);

    // Wake up walsenders for cascading replication
    if (AllowCascadeReplication()) {
        WalSndWakeup(switchedTLI, true);
    }

    // Force WAL receiver reply if requested
    if (doRequestWalReceiverReply) {
        doRequestWalReceiverReply = false;
        WalRcvForceReply();
    }

    // Check if recovery is now consistent
    CheckRecoveryConsistency();

    // Clean up old timeline files if we switched
    if (switchedTLI) {
        RemoveNonParentXlogFiles(xlogreader->EndRecPtr, *replayTLI);
        XLogPrefetchReconfigure();
    }
}
```

Key simplifications made:
- Removed detailed error handling comments for clarity
- Simplified variable declarations and initialization
- Consolidated spin lock operations with clear comments
- Reduced verbose timeline switch logic to essential steps
- Abstracted complex conditional checks into clear flow
- Shortened lengthy comment blocks while preserving key information
- Maintained all critical functionality and error handling paths