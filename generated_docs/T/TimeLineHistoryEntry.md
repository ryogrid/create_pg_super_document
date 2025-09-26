# TimeLineHistoryEntry

## Location
[src/include/access/timeline.h:30-44](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/timeline.h#L30-L44)

## Overview
A data structure that represents a single entry in PostgreSQL's timeline history, tracking the validity range of a timeline within the Write-Ahead Log (WAL) sequence.

## Definition

```c
typedef struct
{
	TimeLineID	tli;
	XLogRecPtr	begin;			/* inclusive */
	XLogRecPtr	end;			/* exclusive, InvalidXLogRecPtr means infinity */
} TimeLineHistoryEntry;
```
## Detailed Description
TimeLineHistoryEntry is a fundamental structure used in PostgreSQL's point-in-time recovery and timeline management system. Each entry represents a contiguous segment of WAL records that belong to a specific timeline. Timeline history is crucial for maintaining database consistency across recovery operations, backup restorations, and replication scenarios.

The structure forms the building blocks of timeline history lists, which track how database timelines have branched and evolved over time. When PostgreSQL performs point-in-time recovery or restores from a backup, it creates new timelines to distinguish the new WAL sequence from the original timeline, preventing confusion and maintaining data integrity.

Timeline history entries are typically organized in lists where each entry represents a chronological segment of database operations within a specific timeline. The begin and end fields define the exact WAL position range where the timeline was active, enabling precise navigation through the database's recovery history.

## Parameters / Member Variables
- : The Timeline ID (uint32) that identifies which specific timeline this entry represents
- : The starting WAL position (XLogRecPtr/uint64) where this timeline segment begins, inclusive of this position
- : The ending WAL position (XLogRecPtr/uint64) where this timeline segment ends, exclusive of this position; InvalidXLogRecPtr indicates the timeline extends infinitely (current/active timeline)

## Dependencies
- Functions called/Symbols referenced:
  - [readTimeLineHistory](../r/readTimeLineHistory.md)
  - [existsTimeLineHistory](../e/existsTimeLineHistory.md)
  - [findNewestTimeLine](../f/findNewestTimeLine.md)
  - [writeTimeLineHistory](../w/writeTimeLineHistory.md)
  - [writeTimeLineHistoryFile](../w/writeTimeLineHistoryFile.md)
  - [restoreTimeLineHistoryFiles](../r/restoreTimeLineHistoryFiles.md)
  - [tliInHistory](../t/tliInHistory.md)
  - [tliOfPointInHistory](../t/tliOfPointInHistory.md)
  - [tliSwitchPoint](../t/tliSwitchPoint.md)

- Called from (representative examples):
  - [readTimeLineHistory](../r/readTimeLineHistory.md) (creates and populates entries from timeline history files)
  - [rescanLatestTimeLine](../r/rescanLatestTimeLine.md) (uses entries to determine latest timeline state)
  - [PrepareForIncrementalBackup](../P/PrepareForIncrementalBackup.md) (processes timeline history for backup operations)
  - [rewind_parseTimeLineHistory](../r/rewind_parseTimeLineHistory.md) (parses timeline entries in pg_rewind utility)
  - [findCommonAncestorTimeline](../f/findCommonAncestorTimeline.md) (compares timeline entries to find common ancestors)

## Notes and Other Information
- Timeline 1 (the original timeline) does not have a history file, so special handling creates a single entry with begin=end=InvalidXLogRecPtr
- Timeline history entries are typically stored in lists with the newest timeline first (reverse chronological order)
- The 'end' field being InvalidXLogRecPtr indicates this is the current active timeline tip
- Timeline IDs must be in strictly increasing sequence within history files to maintain consistency
- Used extensively in recovery, replication, and backup operations to track WAL segment provenance
- Critical for pg_rewind utility operations when rewinding a database to a common ancestor timeline
- Each timeline branch creates a new timeline history file containing all previous timeline entries plus the new branching information