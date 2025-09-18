# CreateCheckPoint

## Location
src/backend/access/transam/xlog.c: 6863 - 7368

## Overview
Performs a comprehensive checkpoint operation that ensures data consistency by flushing all dirty buffers to disk, recording critical system state, and managing WAL records for both online and shutdown scenarios.

## Definition


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
  - CheckPointGuts (core checkpoint work)
  - SyncPreCheckpoint/SyncPostCheckpoint (storage manager coordination)
  - WALInsertLockAcquireExclusive/WALInsertLockRelease (WAL coordination)
  - XLogInsert/XLogFlush (WAL record management)
  - UpdateControlFile (control file updates)
  - LogCheckpointStart/LogCheckpointEnd (checkpoint logging)
  - RemoveOldXlogFiles/PreallocXlogFiles (WAL file management)
  - GetVirtualXIDsDelayingChkpt (transaction coordination)
  - update_checkpoint_display (process status updates)
- Called from (representative examples):
  - CheckpointerMain
  - ShutdownXLOG
  - RequestCheckpoint

## Notes and Other Information
- May take many minutes to execute on busy systems due to extensive I/O operations
- Uses critical sections for atomic control file updates but exits them during I/O-intensive operations
- Implements sophisticated transaction delay handling to ensure consistency with concurrent commits
- Manages replication slot synchronization and WAL segment cleanup
- Updates various system catalogs and maintains checkpoint statistics
- Handles timeline management for point-in-time recovery scenarios
- Process status is updated throughout execution for monitoring purposes
- Skip logic prevents unnecessary checkpoints when system is idle