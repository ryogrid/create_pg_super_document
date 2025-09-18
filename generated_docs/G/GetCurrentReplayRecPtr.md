# GetCurrentReplayRecPtr

## Location
src/backend/access/transam/xlogrecovery.c: 4563 - 4585

## Overview
Retrieves the position of the last applied WAL record or the record currently being applied, providing real-time recovery position including in-progress records.

## Definition
```c
XLogRecPtr GetCurrentReplayRecPtr(TimeLineID *replayEndTLI)
```

## Detailed Description
This function returns the current replay position, which differs from GetXLogReplayRecPtr() in that it includes WAL records that are currently being applied. While GetXLogReplayRecPtr() returns only the position of fully completed replay operations, this function provides the position that includes records in the process of being applied.

The function accesses the `replayEndRecPtr` field from shared memory, which represents the end position of the record currently being processed (or the last completed record if no record is currently being processed). This makes it useful for scenarios where you need the most up-to-date position, including partially applied records.

Like other position-retrieval functions, it uses spinlocks to ensure thread-safe access to shared memory variables and can optionally return the associated timeline ID.

## Parameters / Member Variables
- `replayEndTLI`: Optional output parameter (can be NULL) that receives the timeline ID associated with the current replay position

Return value:
- `XLogRecPtr`: The current replay position including records being applied

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire
  - SpinLockRelease
  - XLogRecoveryCtl->info_lck (spinlock)
  - XLogRecoveryCtl->replayEndRecPtr (shared memory variable)
  - XLogRecoveryCtl->replayEndTLI (shared memory variable)
- Called from (representative examples):
  - UpdateMinRecoveryPoint
  - xlog_redo (multiple locations)
  - Referenced in EndOfWalRecoveryInfo

## Notes and Other Information
- Differs from GetXLogReplayRecPtr() by including records currently being applied
- Critical for maintaining accurate minimum recovery points during WAL replay
- Used primarily by internal recovery mechanisms rather than external monitoring
- Thread-safe implementation using spinlock protection
- Essential for crash recovery scenarios where precise position tracking is required
- The timeline ID parameter allows atomic retrieval of both position and timeline information
- Primarily used within xlog_redo operations for maintaining recovery consistency