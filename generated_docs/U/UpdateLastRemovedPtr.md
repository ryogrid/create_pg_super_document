# UpdateLastRemovedPtr

## Location
[src/backend/access/transam/xlog.c:3789-3808](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L3789-L3808)

## Overview
UpdateLastRemovedPtr updates the shared memory tracking of the last removed WAL segment number when a WAL file has been removed from the system.

## Definition
```c
static void UpdateLastRemovedPtr(char *filename)
```

## Detailed Description
This function maintains critical bookkeeping information about WAL segment removal by updating the shared lastRemovedSegNo field in the WAL control structure. When WAL segments are removed from the filesystem during normal maintenance operations, this function ensures that the system's internal tracking remains consistent with the actual state of available segments. It parses the provided filename to extract the segment number and updates the shared memory pointer only if the removed segment has a higher number than the previously recorded value.

The function employs proper concurrency control by acquiring a spin lock before modifying shared state, ensuring thread-safe updates to the lastRemovedSegNo tracking variable. This information is subsequently used by other parts of the system to determine WAL segment availability and prevent access to segments that have been removed.

## Parameters / Member Variables
- `filename`: char* containing the name of the WAL file that has been removed, used to extract segment number information

## Dependencies
- Functions called/Symbols referenced:
  - [XLogFromFileName](../X/XLogFromFileName.md): Extracts timeline ID and segment number from WAL filename
  - SpinLockAcquire: Acquires spin lock for thread-safe access to shared data
  - SpinLockRelease: Releases spin lock after updating shared data
  - XLogSegNo: Data type for WAL segment numbers
- Called from (representative examples):
  - RefreshXLogWriteResult: During WAL write result updates when segments are removed
  - [RemoveOldXlogFiles](../R/RemoveOldXlogFiles.md): During WAL segment cleanup operations

## Notes and Other Information
- Static function with internal linkage, used only within the xlog.c module
- Updates shared memory state only when the removed segment number is higher than the current tracked value
- Essential for maintaining consistency between filesystem state and internal WAL tracking
- Provides thread-safe access to shared WAL control structures through spin lock protection
- The updated lastRemovedSegNo value is used by functions like CheckXLogRemoved and XLogGetLastRemovedSegno
- Critical component in the WAL segment lifecycle management system

## Simplified Source

```c
// Simplified version of UpdateLastRemovedPtr
static void UpdateLastRemovedPtr(char *filename) {
    uint32 tli;
    XLogSegNo segno;

    // Extract segment number from filename
    XLogFromFileName(filename, &tli, &segno, wal_segment_size);

    // Thread-safe update of shared memory tracking
    SpinLockAcquire(&XLogCtl->info_lck);
    if (segno > XLogCtl->lastRemovedSegNo)
        XLogCtl->lastRemovedSegNo = segno;
    SpinLockRelease(&XLogCtl->info_lck);
}
```

Key simplifications made:
- Focused on the core operation: parse filename and update shared memory
- Preserved essential thread safety with spin lock protection
- Emphasized the conditional update logic (only update if segment number is higher)
- Removed complexity while maintaining the essential tracking functionality