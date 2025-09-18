# SetCurrentChunkStartTime

## Location
src/backend/access/transam/xlogrecovery.c: 4615 - 4626

## Overview
SetCurrentChunkStartTime saves the timestamp marking the beginning of the next chunk of WAL records to be applied during recovery.

## Definition
```c
static void SetCurrentChunkStartTime(TimestampTz xtime)
```

## Detailed Description
This function stores the timestamp that marks the start time of the current chunk of WAL records that will be processed during recovery. The function ensures thread-safe access by using spinlock protection when updating the currentChunkStartTime field in the shared XLogRecoveryCtl structure. This information is stored in shared memory rather than a static variable so that it can be accessed by all backend processes, not just the startup process.

## Parameters / Member Variables
- `xtime`: The timestamp marking the start of the current chunk of WAL records to be processed

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire (for thread-safe access)
  - SpinLockRelease (for thread-safe access)
  - XLogRecoveryCtl (global recovery control structure)
- Called from (representative examples):
  - [WaitForWALToBecomeAvailable](../W/WaitForWALToBecomeAvailable.md)

## Notes and Other Information
- This is a static function, accessible only within the xlogrecovery.c file
- Uses spinlock mechanism to ensure thread-safe updates to shared recovery state
- The timestamp is stored in currentChunkStartTime field of XLogRecoveryCtl
- Part of WAL processing coordination, helping track progress through WAL record chunks
- Enables all backends to see the current chunk processing status, not just the startup process