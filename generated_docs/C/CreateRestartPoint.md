# CreateRestartPoint

## Location
src/backend/access/transam/xlog.c: 7585 - 7880

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
  - RecoveryInProgress
  - UpdateMinRecoveryPoint
  - CheckPointGuts
  - KeepLogSeg
  - GetWalRcvFlushRecPtr
  - GetXLogReplayRecPtr
  - InvalidateObsoleteReplicationSlots
  - RemoveOldXlogFiles
  - PreallocXlogFiles
  - TruncateSUBTRANS
  - ExecuteRecoveryCommand
- Called from (representative examples):
  - CheckpointerMain (in checkpointer process)
  - ShutdownXLOG (during shutdown)

## Notes and Other Information
- This function must be called by the checkpointer process (B_CHECKPOINTER)
- Returns true if a new restart point was established, false otherwise
- Uses various locks including ControlFileLock and WALInsertLock for thread safety
- Integrates with archive_cleanup_command for external cleanup operations
- The function coordinates with replication slots to ensure WAL segments needed by slots are not removed
- Statistics are collected and logged similar to regular checkpoints
- Hot standby compatibility is maintained through proper minRecoveryPoint handling