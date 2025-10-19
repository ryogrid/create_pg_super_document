# RecoveryRestartPoint

## Location
[src/backend/access/transam/xlog.c:7544-7584](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L7544-L7584)

## Overview
RecoveryRestartPoint evaluates checkpoint records during recovery to determine if they represent safe restart points and stores valid checkpoint information in shared memory for the checkpointer process to use.

## Definition
```c
static void RecoveryRestartPoint(const CheckPoint *checkPoint, XLogReaderState *record)
```

## Detailed Description
This function is called each time a checkpoint record is read from the WAL during recovery. It acts as a filter to determine whether the checkpoint represents a safe restart point. The function performs validation checks and, if the checkpoint is deemed safe, copies the checkpoint record information to shared memory where it can be accessed by the checkpointer process via CreateRestartPoint.

The function implements a critical safety check by refusing to create restart points when there are unresolved references to invalid pages. This prevents potential data consistency issues that could arise if recovery were restarted from a point where cross-references to dropped relations would be lost.

## Parameters / Member Variables
- `checkPoint`: Pointer to the CheckPoint structure containing checkpoint record data including the redo LSN
- `record`: XLogReaderState containing the current position and metadata of the checkpoint record being processed

## Dependencies
- Functions called/Symbols referenced:
  - [XLogHaveInvalidPages](../X/XLogHaveInvalidPages.md)
  - elog (DEBUG2 level)
  - SpinLockAcquire/SpinLockRelease
  - [CheckPoint](../C/CheckPoint.md) (structure)
- Called from (representative examples):
  - [xlog_redo](../x/xlog_redo.md) (when processing XLOG_CHECKPOINT_SHUTDOWN and XLOG_CHECKPOINT_ONLINE records)

## Notes and Other Information
- This function is executed by the startup process during recovery, while CreateRestartPoint is executed by the checkpointer process
- The function uses XLogCtl->info_lck spinlock to safely update shared memory checkpoint information
- Invalid page references are tracked to ensure data consistency across restart points
- The stored checkpoint information includes ReadRecPtr, EndRecPtr, and the complete CheckPoint structure
- This is part of PostgreSQL's crash recovery and restart point mechanism for optimizing recovery time

## Simplified Source

```c
static void RecoveryRestartPoint(const CheckPoint *checkPoint, XLogReaderState *record) {
    // Safety check: Don't create restart point if invalid pages exist
    // This prevents losing cross-references to dropped relations
    if (XLogHaveInvalidPages()) {
        elog(DEBUG2, "skipping restart point due to invalid page references");
        return;
    }

    // Copy checkpoint record to shared memory for checkpointer process
    SpinLockAcquire(&XLogCtl->info_lck);
    XLogCtl->lastCheckPointRecPtr = record->ReadRecPtr;
    XLogCtl->lastCheckPointEndPtr = record->EndRecPtr;
    XLogCtl->lastCheckPoint = *checkPoint;
    SpinLockRelease(&XLogCtl->info_lck);
}
```