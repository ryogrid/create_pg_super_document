# tliSwitchPoint

## Location
[src/backend/access/transam/timeline.c:572-592](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/timeline.c#L572-L592)

## Overview
Finds the WAL position where a timeline branched off from the server's history and optionally returns the timeline ID that followed it.

## Definition
```c
XLogRecPtr tliSwitchPoint(TimeLineID tli, List *history, TimeLineID *nextTLI)
```

## Detailed Description
This function is essential for PostgreSQL's timeline management during recovery and replication operations. It searches through the timeline history to find where a specific timeline ended (branched off) and determines the subsequent timeline.

The function iterates through the timeline history entries looking for the specified timeline ID. When found, it returns the end position of that timeline (the switch point). If the caller provides a nextTLI pointer, the function also sets it to the timeline ID that follows the target timeline in the history.

Special cases handled:
- If the timeline is not found in the server's history, an ERROR is raised
- If the timeline is the current (most recent) timeline, InvalidXLogRecPtr is returned
- The nextTLI parameter is optional and can be NULL if the caller doesn't need the following timeline ID

This function is crucial for determining branch points during timeline switches in recovery, replication setup, and WAL streaming operations.

## Parameters / Member Variables
- `tli`: The timeline ID to search for in the history
- `history`: List of TimeLineHistoryEntry structures representing the server's timeline history  
- `nextTLI`: Optional output parameter to receive the timeline ID that followed the target timeline (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [TimeLineHistoryEntry](../T/TimeLineHistoryEntry.md) (structure access)
- Called from (representative examples):
  - [InitWalRecovery](../I/InitWalRecovery.md) (src/backend/access/transam/xlogrecovery.c:847)
  - [XLogReadDetermineTimeline](../X/XLogReadDetermineTimeline.md) (src/backend/access/transam/xlogutils.c:801)
  - [WalSummarizerMain](../W/WalSummarizerMain.md) (src/backend/postmaster/walsummarizer.c:383)
  - [summarizer_read_local_xlog_page](../s/summarizer_read_local_xlog_page.md) (src/backend/postmaster/walsummarizer.c:1564)
  - [StartReplication](../S/StartReplication.md) (src/backend/replication/walsender.c:897)
  - [XLogSendPhysical](../X/XLogSendPhysical.md) (src/backend/replication/walsender.c:3182)

## Notes and Other Information
- Returns InvalidXLogRecPtr for the current timeline (indicating no branch point exists yet)
- Raises ERROR if the requested timeline is not in the server's history
- The nextTLI parameter tracks the timeline that follows in chronological order
- Critical for replication and recovery operations that need to understand timeline branching
- Used extensively in WAL streaming and physical replication setup
- The function assumes timeline history is properly ordered and complete

## Simplified Source

```c
// Simplified version of tliSwitchPoint
XLogRecPtr tliSwitchPoint(TimeLineID tli, List *history, TimeLineID *nextTLI) {
    ListCell *cell;

    // Initialize output parameter if provided
    if (nextTLI)
        *nextTLI = 0;

    // Search through timeline history
    foreach(cell, history) {
        TimeLineHistoryEntry *tle = (TimeLineHistoryEntry *) lfirst(cell);

        if (tle->tli == tli)
            return tle->end;  // Found the timeline, return its end point

        // Track the next timeline ID for output
        if (nextTLI)
            *nextTLI = tle->tli;
    }

    // Timeline not found in history
    ereport(ERROR, (errmsg("requested timeline %u is not in this server's history", tli)));
    return InvalidXLogRecPtr;  // Keep compiler quiet
}
```

Key simplifications made:
- Added clear comments explaining the search logic
- Simplified the timeline tracking with descriptive comments
- Preserved essential error handling for missing timelines
- Maintained the nextTLI output parameter logic
- Kept the function compact while preserving all functionality