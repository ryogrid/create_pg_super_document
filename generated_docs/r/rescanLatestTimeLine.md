# rescanLatestTimeLine

## Location
[src/backend/access/transam/xlogrecovery.c:4105-4191](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L4105-L4191)

## Overview
Scans for new timelines that might have appeared in the archive since recovery started, and switches the recovery target to the latest valid timeline if found.

## Definition
```c
static bool rescanLatestTimeLine(TimeLineID replayTLI, XLogRecPtr replayLSN)
```

## Detailed Description
This function is called during WAL recovery to check if any new timelines have been created in the archive since recovery began. It validates that any new timeline found is a proper descendant of the current recovery timeline and that the fork point occurred after the current replay position. If a valid new timeline is discovered, the function updates the recovery target timeline and ensures all necessary timeline history files are available.

The function performs several validation steps:
1. Uses `findNewestTimeLine` to discover if there are newer timelines available
2. Reads the timeline history of the new timeline to understand its ancestry
3. Verifies that the current recovery timeline is part of the new timeline's history
4. Ensures the fork point occurred after the current recovery position
5. If all validations pass, switches to the new timeline and restores required history files

## Parameters / Member Variables
- `replayTLI`: The current timeline ID being replayed during recovery
- `replayLSN`: The current WAL position (Log Sequence Number) being replayed

## Dependencies
- Functions called/Symbols referenced:
  - [findNewestTimeLine](../f/findNewestTimeLine.md)
  - [readTimeLineHistory](readTimeLineHistory.md)
  - [TimeLineHistoryEntry](../T/TimeLineHistoryEntry.md)
  - [list_free_deep](../l/list_free_deep.md)
  - [restoreTimeLineHistoryFiles](restoreTimeLineHistoryFiles.md)
- Called from (representative examples):
  - [WaitForWALToBecomeAvailable](../W/WaitForWALToBecomeAvailable.md)

## Notes and Other Information
- This is a static function, only accessible within the xlogrecovery.c module
- The function modifies global variables `recoveryTargetTLI` and `expectedTLEs` when switching to a new timeline
- Timeline switching is only allowed if the new timeline is a proper descendant and the fork occurred after the current replay position
- Logs informational messages when timeline switches occur or when invalid timelines are detected
- Returns `true` if a timeline switch occurred, `false` otherwise

## Simplified Source
```c
static bool rescanLatestTimeLine(TimeLineID replayTLI, XLogRecPtr replayLSN)
{
    TimeLineID newtarget;
    TimeLineID oldtarget = recoveryTargetTLI;
    List *newExpectedTLEs;
    bool found = false;

    // Check if a newer timeline exists
    newtarget = findNewestTimeLine(recoveryTargetTLI);
    if (newtarget == recoveryTargetTLI) {
        return false; // No new timelines found
    }

    // Read history of the new timeline
    newExpectedTLEs = readTimeLineHistory(newtarget);

    // Verify current timeline is part of new timeline's history
    foreach(cell, newExpectedTLEs) {
        TimeLineHistoryEntry *currentTle = (TimeLineHistoryEntry *) lfirst(cell);
        if (currentTle->tli == recoveryTargetTLI) {
            found = true;
            // Check fork point is after current replay position
            if (currentTle->end < replayLSN) {
                ereport(LOG, (errmsg("timeline forked before current recovery point")));
                return false;
            }
            break;
        }
    }

    if (!found) {
        ereport(LOG, (errmsg("new timeline is not a child of current timeline")));
        return false;
    }

    // Switch to new timeline
    recoveryTargetTLI = newtarget;
    list_free_deep(expectedTLEs);
    expectedTLEs = newExpectedTLEs;

    // Restore timeline history files
    restoreTimeLineHistoryFiles(oldtarget + 1, newtarget);

    ereport(LOG, (errmsg("switched to timeline %u", recoveryTargetTLI)));
    return true;
}
```