# GetRedoRecPtr

## Location
[src/backend/access/transam/xlog.c:6416-6445](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L6416-L6445)

## Overview
GetRedoRecPtr returns the current Redo pointer from shared memory and updates the local RedoRecPtr copy as a side effect.

## Definition

```c
XLogRecPtr
GetRedoRecPtr(void)
```
## Detailed Description
GetRedoRecPtr retrieves the current Redo record pointer from PostgreSQL's shared memory control structure (XLogCtl). The Redo pointer indicates the earliest WAL record that might need to be replayed during recovery. The function uses spinlock protection for thread-safe access to shared memory. As a performance optimization and consistency measure, it also updates the local process copy (RedoRecPtr) to ensure it's at least as recent as the shared value. The function deliberately uses a potentially stale copy from XLogCtl rather than acquiring WAL insertion locks, as the value could change immediately after lock release anyway.

## Parameters / Member Variables
- Returns: XLogRecPtr (the current Redo record pointer)

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire (on XLogCtl->info_lck)
  - SpinLockRelease (on XLogCtl->info_lck)
  - XLogCtl (global shared memory structure)
  - RedoRecPtr (local static variable)
- Called from (representative examples):
  - [CheckPointLogicalRewriteHeap](../C/CheckPointLogicalRewriteHeap.md)
  - [XLogWrite](../X/XLogWrite.md)
  - [XLogSaveBufferForHint](../X/XLogSaveBufferForHint.md)
  - [XLogPageRead](../X/XLogPageRead.md)
  - [nextval_internal](../n/nextval_internal.md)
  - [MaybeRemoveOldWalSummaries](../M/MaybeRemoveOldWalSummaries.md)
  - [CheckPointSnapBuild](../C/CheckPointSnapBuild.md)
  - [ReplicationSlotReserveWal](../R/ReplicationSlotReserveWal.md)
  - [smgr_bulk_start_smgr](../s/smgr_bulk_start_smgr.md)
  - [smgr_bulk_finish](../s/smgr_bulk_finish.md)
  - [WALAvailability](../W/WALAvailability.md)

## Notes and Other Information
- Updates the local RedoRecPtr copy as a side effect for performance optimization
- Uses spinlock protection for thread-safe shared memory access
- Intentionally uses potentially stale copy from XLogCtl for performance reasons
- Critical for determining WAL cleanup boundaries and recovery points
- The Redo pointer represents the earliest WAL record that might need replay
- Located in src/backend/access/transam/xlog.c:6416-6445
- Widely used across PostgreSQL subsystems including replication, storage management, and recovery

## Simplified Source

```c
// Simplified version of GetRedoRecPtr
XLogRecPtr GetRedoRecPtr(void) {
    XLogRecPtr shared_redo_ptr;

    // Step 1: Get current Redo pointer from shared memory with spinlock protection
    SpinLockAcquire(&XLogCtl->info_lck);
    shared_redo_ptr = XLogCtl->RedoRecPtr;
    SpinLockRelease(&XLogCtl->info_lck);

    // Step 2: Update local copy if shared value is newer
    if (RedoRecPtr < shared_redo_ptr) {
        RedoRecPtr = shared_redo_ptr;
    }

    // Step 3: Return the current (possibly updated) local copy
    return RedoRecPtr;
}
```

Key simplifications made:
- Renamed variable for clarity (ptr -> shared_redo_ptr)
- Added step-by-step comments explaining the logic
- Removed the detailed comment about WAL insertion locks for brevity
- Preserved the essential thread-safety and update logic
- Maintained the side-effect of updating the local RedoRecPtr copy