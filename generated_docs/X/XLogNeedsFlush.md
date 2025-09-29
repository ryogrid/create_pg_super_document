# XLogNeedsFlush

## Location
[src/backend/access/transam/xlog.c:3110-3186](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L3110-L3186)

## Overview
Tests whether WAL data has been flushed up to a given position, handling both normal operation and recovery scenarios with different semantics for each mode.

## Definition

```c
bool
XLogNeedsFlush(XLogRecPtr record)
```
## Detailed Description
XLogNeedsFlush is a utility function that determines whether a flush operation is needed to ensure a specific WAL position has been made durable. The function has fundamentally different behavior depending on whether the system is in recovery or normal operation:

**During Recovery:**
- Instead of checking WAL flush status, it checks whether the minimum recovery point needs updating
- Uses local caching of minRecoveryPoint to avoid frequent control file access
- Implements optimistic locking with LWLockConditionalAcquire to avoid blocking when the control file lock is busy
- Returns a conservative estimate (true) when the lock cannot be acquired immediately
- Handles crash recovery specially by disabling updates when minRecoveryPoint is invalid

**During Normal Operation:**
- Checks if the requested LSN has already been flushed to disk
- Uses cached LogwrtResult for quick initial checks
- Refreshes the write result if the initial check suggests a flush might be needed
- Performs a final verification after refreshing the cached state

The function is designed to be lightweight and non-blocking, making it suitable for frequent calls from performance-critical code paths.

## Parameters / Member Variables
- : The WAL log sequence number to check for flush status
- Returns:  - true if a flush is still needed, false if the position is already durable

## Dependencies
- Functions called/Symbols referenced:
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - XLogRecPtrIsInvalid
  - [LWLockConditionalAcquire](../L/LWLockConditionalAcquire.md)
  - RefreshXLogWriteResult
- Called from (representative examples):
  - [SetHintBits](../S/SetHintBits.md)
  - [GetVictimBuffer](../G/GetVictimBuffer.md)

## Notes and Other Information
- Uses different semantics during recovery (minRecoveryPoint) vs normal operation (WAL flush)
- Implements optimistic locking to avoid blocking on busy control file locks
- Uses local caching to minimize expensive control file operations
- Returns conservative estimates when precise information is not immediately available
- Critical for buffer management decisions and hint bit setting operations
- The updateMinRecoveryPoint flag can be disabled during crash recovery for safety

## Simplified Source

```c
bool
XLogNeedsFlush(XLogRecPtr record)
{
    // During recovery: check if minRecoveryPoint needs updating
    if (RecoveryInProgress())
    {
        // Crash recovery optimization: disable updates if minRecoveryPoint invalid
        if (XLogRecPtrIsInvalid(LocalMinRecoveryPoint) && InRecovery)
            updateMinRecoveryPoint = false;

        // Quick exit if already updated or updates disabled
        if (record <= LocalMinRecoveryPoint || !updateMinRecoveryPoint)
            return false;

        // Try to update local minRecoveryPoint (non-blocking)
        if (!LWLockConditionalAcquire(ControlFileLock, LW_SHARED))
            return true;  // Conservative guess if lock busy

        LocalMinRecoveryPoint = ControlFile->minRecoveryPoint;
        LocalMinRecoveryPointTLI = ControlFile->minRecoveryPointTLI;
        LWLockRelease(ControlFileLock);

        // Final check after update
        if (record <= LocalMinRecoveryPoint || !updateMinRecoveryPoint)
            return false;
        else
            return true;
    }

    // Normal operation: check WAL flush status

    // Quick exit if already flushed
    if (record <= LogwrtResult.Flush)
        return false;

    // Refresh cached flush state
    RefreshXLogWriteResult(LogwrtResult);

    // Final check after refresh
    if (record <= LogwrtResult.Flush)
        return false;

    return true;
}
```