# CreateEndOfRecoveryRecord

## Location
[src/backend/access/transam/xlog.c:7369-7433](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L7369-L7433)

## Overview
Marks the end of WAL recovery by inserting a special end-of-recovery record without performing a full checkpoint, allowing the system to transition from recovery to normal operation.

## Definition

```c
static void
CreateEndOfRecoveryRecord(void)
```
## Detailed Description
CreateEndOfRecoveryRecord creates a lightweight end-of-recovery marker in the WAL stream that signifies the completion of crash recovery or archive recovery. Unlike a full checkpoint, this function only writes a single WAL record (XLOG_END_OF_RECOVERY) containing essential timeline and timestamp information.

The function operates independently of ongoing restartpoint operations and is designed to be non-blocking. It captures the current timeline information while holding WAL insertion locks exclusively to ensure consistency, then updates the control file to record the minimum recovery point for future crash recovery scenarios.

This mechanism allows PostgreSQL to transition from recovery mode to normal operation more efficiently than waiting for a complete checkpoint, while still maintaining the necessary recovery guarantees.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [RecoveryInProgress](../R/RecoveryInProgress.md) (recovery state check)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md) (timestamp capture)
  - [WALInsertLockAcquireExclusive](../W/WALInsertLockAcquireExclusive.md)/WALInsertLockRelease (WAL coordination)
  - [XLogBeginInsert](../X/XLogBeginInsert.md)/XLogRegisterData/XLogInsert (WAL record creation)
  - [XLogFlush](../X/XLogFlush.md) (WAL persistence)
  - [UpdateControlFile](../U/UpdateControlFile.md) (control file updates)
  - [xl_end_of_recovery](../x/xl_end_of_recovery.md) (record structure)
  - XLOG_END_OF_RECOVERY (record type constant)
- Called from (representative examples):
  - RefreshXLogWriteResult
  - [PerformRecoveryXLogAction](../P/PerformRecoveryXLogAction.md)

## Notes and Other Information
- Only callable during recovery mode; validates this with RecoveryInProgress()
- Operates independently of concurrent restartpoint operations
- Updates control file's minRecoveryPoint to enable proper timeline handling in future recovery
- Uses critical sections to ensure atomicity of control file updates
- Timeline information is captured atomically under WAL insertion locks
- More lightweight alternative to full checkpoint for ending recovery
- Essential for proper point-in-time recovery and timeline switching functionality

## Simplified Source

```c
// Simplified version of CreateEndOfRecoveryRecord
static void
CreateEndOfRecoveryRecord(void)
{
    xl_end_of_recovery xlrec;
    XLogRecPtr recptr;

    // Sanity check: ensure we're actually in recovery
    if (!RecoveryInProgress())
        elog(ERROR, "can only be used to end recovery");

    // Prepare end-of-recovery record with current state
    xlrec.end_time = GetCurrentTimestamp();
    xlrec.wal_level = wal_level;

    // Capture timeline information atomically
    WALInsertLockAcquireExclusive();
    xlrec.ThisTimeLineID = XLogCtl->InsertTimeLineID;
    xlrec.PrevTimeLineID = XLogCtl->PrevTimeLineID;
    WALInsertLockRelease();

    // Write the end-of-recovery WAL record
    START_CRIT_SECTION();
    XLogBeginInsert();
    XLogRegisterData((char *) &xlrec, sizeof(xl_end_of_recovery));
    recptr = XLogInsert(RM_XLOG_ID, XLOG_END_OF_RECOVERY);
    XLogFlush(recptr);

    // Update control file for future crash recovery
    LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
    ControlFile->minRecoveryPoint = recptr;
    ControlFile->minRecoveryPointTLI = xlrec.ThisTimeLineID;
    UpdateControlFile();
    LWLockRelease(ControlFileLock);
    END_CRIT_SECTION();
}
```

Key simplifications made:
- Added clear step-by-step comments explaining the major phases
- Preserved all essential logic and error checking
- Maintained the critical section structure for atomicity
- Kept the timeline information capture under WAL locks
- Simplified variable declarations while preserving functionality
- Added explanatory comments for each major operation group