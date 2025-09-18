# XLogRecoveryCtlData

## Location
[src/backend/access/transam/xlogrecovery.c:304-362](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L304-L362)

## Overview
XLogRecoveryCtlData is a shared-memory structure that maintains the state and control information for WAL (Write-Ahead Log) recovery operations in PostgreSQL.

## Definition


## Detailed Description
This structure serves as the central control hub for WAL recovery operations in shared memory, coordinating recovery state across multiple processes. It tracks the progress of WAL replay, manages hot standby operations, handles promotion triggers, and provides synchronization mechanisms for recovery processes.

The structure is critical for crash recovery, archive recovery, and streaming replication scenarios, maintaining essential state information that allows the system to properly coordinate recovery operations, handle standby queries, and manage recovery pausing and resumption.

## Parameters / Member Variables
- : Boolean flag indicating whether hot standby queries are currently allowed to run (protected by info_lck)
- : Boolean flag indicating if a standby promotion has been triggered (protected by info_lck)
- : Latch used to wake up the startup process to continue WAL replay when waiting for WAL arrival or promotion requests
- : Start position of the last record successfully replayed
- : End+1 position of the last record successfully replayed
- : Timeline ID of the last successfully replayed record
- : End+1 position of the record currently being replayed, or equal to lastReplayedEndRecPtr when not actively replaying
- : Timeline ID for the record currently being replayed
- : Timestamp of the last COMMIT/ABORT record replayed or being replayed
- : Timestamp when the current chunk of WAL data started being replayed (relevant for replication/archive recovery)
- : Current state of recovery pause functionality
- : Condition variable for coordinating recovery pause/resume operations
- : Spinlock that protects the shared variables in this structure

## Dependencies
- Functions called/Symbols referenced:
  - [Latch](../L/Latch.md)
  - [RecoveryPauseState](../R/RecoveryPauseState.md)
  - ConditionVariable
  - [slock_t](../s/slock_t.md)
  - XLogRecPtr
  - TimeLineID
  - TimestampTz
- Called from (representative examples):
  - [XLogRecoveryShmemSize](XLogRecoveryShmemSize.md)
  - [XLogRecoveryShmemInit](XLogRecoveryShmemInit.md)
  - [PerformWalRecovery](../P/PerformWalRecovery.md)
  - [WaitForWALToBecomeAvailable](../W/WaitForWALToBecomeAvailable.md)

## Notes and Other Information
This structure is allocated in shared memory and is crucial for multi-process coordination during recovery operations. The recoveryWakeupLatch is specifically designed as a separate latch from the startup process's procLatch to maintain clear separation between WAL replay signaling and recovery conflict handling. The structure supports both hot standby operations and promotion scenarios, making it essential for streaming replication setups.