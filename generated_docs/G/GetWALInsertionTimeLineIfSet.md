# GetWALInsertionTimeLineIfSet

## Location
[src/backend/access/transam/xlog.c:6515-6534](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L6515-L6534)

## Overview
Returns the WAL insertion timeline if the system is not in recovery, otherwise returns 0, providing a safe way to check timeline availability during recovery transitions.

## Definition
TimeLineID GetWALInsertionTimeLineIfSet(void)

## Detailed Description
GetWALInsertionTimeLineIfSet provides a safe way to access the WAL insertion timeline that works correctly during recovery state transitions. Unlike GetWALInsertionTimeLine(), this function does not assert that recovery is complete, making it suitable for use in scenarios where the recovery state might be transitioning.

The function uses spinlock protection to safely read the InsertTimeLineID from the shared XLogCtl structure. It returns the timeline ID if it has been set (indicating that recovery has progressed far enough to establish the insertion timeline), or 0 if the timeline is not yet available. The function detects that recovery has ended as soon as the insert timeline is set, which happens before the SharedRecoveryState is updated to RECOVERY_STATE_DONE.

The documentation notes recommend using GetWALInsertionTimeLine() instead wherever possible, since it is cheaper due to not requiring spinlock acquisition when recovery is known to be complete.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire
  - SpinLockRelease
  - XLogCtl (global WAL control structure)
- Called from (representative examples):
  - [GetLatestLSN](GetLatestLSN.md)
  - [WALAvailability](../W/WALAvailability.md)

## Notes and Other Information
- Safe to call during recovery state transitions, unlike GetWALInsertionTimeLine()
- Returns 0 if timeline is not yet set (system still in recovery)
- Uses spinlock protection for safe concurrent access
- More expensive than GetWALInsertionTimeLine() due to locking overhead
- Detects recovery completion earlier than SharedRecoveryState updates
- Should be used only when recovery state is uncertain
- Located in src/backend/access/transam/xlog.c:6515-6534

## Simplified Source

```c
TimeLineID GetWALInsertionTimeLineIfSet(void)
{
    TimeLineID insertTLI;

    // Safely read the insertion timeline ID with spinlock protection
    SpinLockAcquire(&XLogCtl->info_lck);
    insertTLI = XLogCtl->InsertTimeLineID;
    SpinLockRelease(&XLogCtl->info_lck);

    return insertTLI;  // Returns 0 if not set (still in recovery)
}
```