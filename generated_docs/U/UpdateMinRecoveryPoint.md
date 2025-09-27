# UpdateMinRecoveryPoint

## Location
[src/backend/access/transam/xlog.c:2699-2778](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L2699-L2778)

## Overview
Updates the minimum recovery point in the control file to ensure database consistency during crash recovery, tracking the WAL location that must be reached before the database can be considered consistent.

## Definition

```c
static void
UpdateMinRecoveryPoint(XLogRecPtr lsn, bool force)
```
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
  - [UpdateControlFile](UpdateControlFile.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease
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

## Simplified Source

```c
// Simplified version of UpdateMinRecoveryPoint
static void UpdateMinRecoveryPoint(XLogRecPtr lsn, bool force) {
    // Quick check: Skip if updates disabled or LSN not advancing
    if (!updateMinRecoveryPoint || (!force && lsn <= LocalMinRecoveryPoint))
        return;

    // Skip updates during crash recovery to ensure all WAL is replayed
    if (XLogRecPtrIsInvalid(LocalMinRecoveryPoint) && InRecovery) {
        updateMinRecoveryPoint = false;
        return;
    }

    // Acquire exclusive lock on control file
    LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);

    // Update local copies from control file
    LocalMinRecoveryPoint = ControlFile->minRecoveryPoint;
    LocalMinRecoveryPointTLI = ControlFile->minRecoveryPointTLI;

    // Check if update is needed
    if (XLogRecPtrIsInvalid(LocalMinRecoveryPoint)) {
        updateMinRecoveryPoint = false;
    } else if (force || LocalMinRecoveryPoint < lsn) {
        // Get current replay position (protects against bogus LSNs)
        XLogRecPtr newMinRecoveryPoint = GetCurrentReplayRecPtr(&newMinRecoveryPointTLI);

        // Log warning if requested LSN is beyond current replay point
        if (!force && newMinRecoveryPoint < lsn)
            elog(WARNING, "xlog min recovery request is past current point");

        // Update control file if new point is higher
        if (ControlFile->minRecoveryPoint < newMinRecoveryPoint) {
            ControlFile->minRecoveryPoint = newMinRecoveryPoint;
            ControlFile->minRecoveryPointTLI = newMinRecoveryPointTLI;
            UpdateControlFile();

            // Update local copies
            LocalMinRecoveryPoint = newMinRecoveryPoint;
            LocalMinRecoveryPointTLI = newMinRecoveryPointTLI;
        }
    }

    LWLockRelease(ControlFileLock);
}
```

Key simplifications made:
- Removed detailed comments and consolidated them into brief explanatory comments
- Simplified variable declarations (moved TimeLineID declaration inline)
- Condensed the warning message format
- Removed the detailed debug reporting
- Focused on the main execution flow while preserving all essential logic
- Maintained the critical safety checks and error handling