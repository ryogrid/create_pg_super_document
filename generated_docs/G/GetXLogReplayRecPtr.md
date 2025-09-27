# GetXLogReplayRecPtr

## Location
[src/backend/access/transam/xlogrecovery.c:4540-4562](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L4540-L4562)

## Overview
Retrieves the latest WAL replay position and optionally the associated timeline ID, providing safe access to the current recovery progress.

## Definition
```c
XLogRecPtr GetXLogReplayRecPtr(TimeLineID *replayTLI)
```

## Detailed Description
This function returns the latest position (LSN - Log Sequence Number) in the WAL where recovery has been applied, along with the associated timeline ID if requested. It provides a thread-safe way to access the current replay position by acquiring the appropriate spinlock before reading shared memory variables.

The function is specifically exported to allow components like WALReceiver to read the replay position directly. This is crucial for replication scenarios where various processes need to know how far recovery has progressed, enabling coordination between primary and standby servers.

The replay position represents the end of the last successfully applied WAL record, making it safe to use this position for various replication and recovery operations.

## Parameters / Member Variables
- `replayTLI`: Optional output parameter (can be NULL) that receives the timeline ID associated with the replay position

Return value:
- `XLogRecPtr`: The latest WAL replay position (LSN)

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire
  - SpinLockRelease
  - XLogRecoveryCtl->info_lck (spinlock)
  - XLogRecoveryCtl->lastReplayedEndRecPtr (shared memory variable)
  - XLogRecoveryCtl->lastReplayedTLI (shared memory variable)
- Called from (representative examples):
  - [CreateRestartPoint](../C/CreateRestartPoint.md)
  - [pg_last_wal_replay_lsn](../p/pg_last_wal_replay_lsn.md)
  - [WalReceiverMain](../W/WalReceiverMain.md)
  - [XLogWalRcvSendReply](../X/XLogWalRcvSendReply.md)
  - [GetReplicationApplyDelay](GetReplicationApplyDelay.md)
  - [WalSndWaitForWal](../W/WalSndWaitForWal.md)

## Notes and Other Information
- Thread-safe function that uses spinlocks to protect shared memory access
- Critical for replication coordination between primary and standby servers
- Used by SQL functions like pg_last_wal_replay_lsn() to expose replay progress to users
- Essential for WAL receiver processes to track and communicate recovery progress
- The timeline ID parameter allows callers to get both position and timeline atomically
- Widely used throughout PostgreSQL's replication and recovery infrastructure

## Simplified Source

```c
// Simplified version of GetXLogReplayRecPtr
XLogRecPtr GetXLogReplayRecPtr(TimeLineID *replayTLI) {
    XLogRecPtr replay_position;
    TimeLineID timeline_id;

    // Safely read shared recovery state with spinlock protection
    SpinLockAcquire(&XLogRecoveryCtl->info_lck);
    replay_position = XLogRecoveryCtl->lastReplayedEndRecPtr;
    timeline_id = XLogRecoveryCtl->lastReplayedTLI;
    SpinLockRelease(&XLogRecoveryCtl->info_lck);

    // Return timeline ID if caller requested it
    if (replayTLI) {
        *replayTLI = timeline_id;
    }

    return replay_position;
}
```

Key simplifications made:
- Used more descriptive variable names (replay_position, timeline_id)
- Added explanatory comments for the main operations
- Preserved the essential thread-safety mechanism
- Maintained the core functionality of atomic read with optional timeline output