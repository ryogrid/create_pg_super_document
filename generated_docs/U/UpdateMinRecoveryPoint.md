# UpdateMinRecoveryPoint

## Location
src/backend/access/transam/xlog.c: 2699 - 2778

## Overview
Updates the minimum recovery point in the control file to ensure database consistency during crash recovery, tracking the WAL location that must be reached before the database can be considered consistent.

## Definition


## Detailed Description
UpdateMinRecoveryPoint is a critical function in PostgreSQL's WAL (Write-Ahead Log) recovery mechanism that manages the minimum recovery point stored in the control file. This point represents the WAL location that must be reached during recovery before the database can be considered consistent and safe to use.

The function implements several important safety mechanisms:
1. **Early exit optimization**: Uses local copies to avoid unnecessary control file updates
2. **Crash recovery handling**: Prevents updates during crash recovery to ensure all available WAL is replayed
3. **Bogus LSN protection**: Guards against corrupted heap page LSNs by using the current replay position instead
4. **Batch updates**: Updates to the furthest replayed position rather than just the requested LSN to minimize control file I/O

The function acquires an exclusive lock on the control file to ensure atomic updates and maintains both the recovery point LSN and its associated timeline ID.

## Parameters / Member Variables
- : The WAL log sequence number that should be the new minimum recovery point
- : If true, ignores the lsn parameter and forces an update to the current replay position

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecPtrIsInvalid
  - [GetCurrentReplayRecPtr](../G/GetCurrentReplayRecPtr.md)
  - UpdateControlFile
  - LWLockAcquire/LWLockRelease
- Called from (representative examples):
  - RefreshXLogWriteResult
  - [XLogFlush](../X/XLogFlush.md)
  - [XLogInitNewTimeline](../X/XLogInitNewTimeline.md)
  - [CreateRestartPoint](../C/CreateRestartPoint.md)

## Notes and Other Information
- Only active during recovery operations (InRecovery must be true)
- Uses local caching (LocalMinRecoveryPoint) to minimize expensive control file operations
- Includes protection against corrupted LSN values from damaged heap pages
- Critical for ensuring ACID properties during crash recovery scenarios
- The updateMinRecoveryPoint flag can be disabled to prevent unnecessary updates during crash recovery