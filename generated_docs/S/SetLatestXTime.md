# SetLatestXTime

## Location
src/backend/access/transam/xlogrecovery.c: 4586 - 4596

## Overview
SetLatestXTime saves the timestamp of the latest processed commit/abort record during PostgreSQL recovery operations.

## Definition
```c
static void SetLatestXTime(TimestampTz xtime)
```

## Detailed Description
This function stores the timestamp of the most recently processed commit or abort record in the shared XLogRecoveryCtl structure. The function is designed to be thread-safe by using spinlock protection when updating the recoveryLastXTime field. It stores this information in XLogRecoveryCtl rather than a static variable so that it can be accessed by processes other than the startup process, particularly the checkpointer process when executing CreateRestartPoint.

## Parameters / Member Variables
- `xtime`: The timestamp of the latest processed commit/abort record to be saved

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire (for thread-safe access)
  - SpinLockRelease (for thread-safe access)
  - XLogRecoveryCtl (global recovery control structure)
- Called from (representative examples):
  - recoveryStopsAfter

## Notes and Other Information
- This is a static function, meaning it is only accessible within the xlogrecovery.c file
- Uses spinlock mechanism to ensure thread-safe updates to the shared recovery control structure
- The timestamp is stored in recoveryLastXTime field of XLogRecoveryCtl
- Critical for recovery coordination between the startup process and checkpointer process