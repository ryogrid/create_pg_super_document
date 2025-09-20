# CreateCheckPoint

## Location
[src/backend/access/transam/xlog.c:6863-7368](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L6863-L7368)

## Overview
Performs a comprehensive checkpoint operation that ensures data consistency by flushing all dirty buffers to disk, recording critical system state, and managing WAL records for both online and shutdown scenarios.

## Definition

```c
structed the checkpoint record, ensure all shmem disk buffers
	 * and commit-log buffers are flushed to disk.
	 *
	 * This I/O could fail for various reasons.  If so, we will fail to
	 * complete the checkpoint, but there is no reason to force a system
	 * panic. Accordingly, exit critical section while doing it.
	 */
	END_CRIT_SECTION();
```
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
  - UpdateControlFile (control file updates)
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