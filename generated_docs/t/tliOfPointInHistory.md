# tliOfPointInHistory

## Location
src/backend/access/transam/timeline.c: 544 - 571

## Overview
Determines which timeline ID was active at a specific WAL position by searching through timeline history entries.

## Definition
```c
TimeLineID tliOfPointInHistory(XLogRecPtr ptr, List *history)
```

## Detailed Description
This function performs a critical lookup operation in PostgreSQL's timeline management system. Given a specific WAL (Write-Ahead Log) record pointer position and a timeline history list, it determines which timeline was active at that point in time.

The function iterates through timeline history entries, checking if the given WAL position falls within the range of each timeline entry. Timeline history entries contain begin and end boundaries that define the WAL range where each timeline was active. The function handles special cases where begin or end positions might be invalid (represented by InvalidXLogRecPtr), treating invalid begin as the start of time and invalid end as continuing indefinitely.

If no matching timeline is found, the function raises an ERROR, as this indicates a corrupted or non-contiguous timeline history, which should never occur in a properly functioning PostgreSQL system.

## Parameters / Member Variables
- `ptr`: WAL record pointer position to look up in the timeline history
- `history`: List of TimeLineHistoryEntry structures representing the timeline history

## Dependencies
- Functions called/Symbols referenced:
  - TimeLineHistoryEntry (structure access)
  - XLogRecPtrIsInvalid (macro for checking invalid WAL positions)
- Called from (representative examples):
  - [InitWalRecovery](../I/InitWalRecovery.md) (src/backend/access/transam/xlogrecovery.c:838, 862)
  - [WaitForWALToBecomeAvailable](../W/WaitForWALToBecomeAvailable.md) (src/backend/access/transam/xlogrecovery.c:3842)
  - [XLogReadDetermineTimeline](../X/XLogReadDetermineTimeline.md) (src/backend/access/transam/xlogutils.c:800)
  - READ_REPLICATION_SLOT_COLS (src/backend/replication/walsender.c:572)

## Notes and Other Information
- Critical for point-in-time recovery and timeline consistency validation
- Assumes timeline history is contiguous - raises ERROR if gaps are found
- Handles invalid XLogRecPtr values (InvalidXLogRecPtr) as open-ended ranges
- Used extensively during WAL recovery to determine correct timeline context
- Timeline ranges are inclusive at the beginning and exclusive at the end
- Function should never return 0 in normal operation - the return 0 is only to suppress compiler warnings