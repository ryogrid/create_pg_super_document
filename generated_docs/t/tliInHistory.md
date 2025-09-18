# tliInHistory

## Location
src/backend/access/transam/timeline.c: 526 - 543

## Overview
Checks if a given timeline ID exists within a list of expected timeline history entries.

## Definition
```c
bool tliInHistory(TimeLineID tli, List *expectedTLEs)
```

## Detailed Description
This function performs a simple linear search through a list of TimeLineHistoryEntry structures to determine whether a specific timeline ID is present. It's a utility function used during WAL recovery and timeline validation to verify that a timeline ID is part of the expected timeline history.

The function iterates through the provided list using PostgreSQL's foreach macro, comparing each timeline history entry's timeline ID against the target timeline ID. It returns immediately upon finding a match, making it efficient for typical use cases where the target timeline is likely to be found early in the list.

## Parameters / Member Variables
- `tli`: The timeline ID to search for in the history list
- `expectedTLEs`: A PostgreSQL List containing TimeLineHistoryEntry structures to search through

## Dependencies
- Functions called/Symbols referenced:
  - TimeLineHistoryEntry (structure access)
- Called from (representative examples):
  - [checkTimeLineSwitch](../c/checkTimeLineSwitch.md) (src/backend/access/transam/xlogrecovery.c:2390)
  - [ReadRecord](../R/ReadRecord.md) (src/backend/access/transam/xlogrecovery.c:3193)

## Notes and Other Information
- Simple O(n) linear search algorithm - adequate for typical timeline history sizes
- Uses PostgreSQL's foreach macro for list iteration
- Returns immediately upon finding the first match (short-circuit evaluation)
- Commonly used during WAL recovery to validate timeline consistency
- The function assumes the expectedTLEs list contains valid TimeLineHistoryEntry pointers