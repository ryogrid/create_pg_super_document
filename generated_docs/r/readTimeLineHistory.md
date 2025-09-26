# readTimeLineHistory

## Location
[src/backend/access/transam/timeline.c:76-221](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/timeline.c#L76-L221)

## Overview
Reads and parses a timeline history file to construct a list of timeline entries representing the branching history of a specific timeline.

## Definition
```c
List *readTimeLineHistory(TimeLineID targetTLI)
```

## Detailed Description
This function reads a timeline history file for a given timeline ID and constructs a list of TimeLineHistoryEntry structures representing the complete timeline history. Timeline history files contain information about when timelines were created and their switchpoint locations in the WAL.

The function handles several scenarios:
1. For timeline 1 (master timeline), returns a single entry since it has no history file
2. During archive recovery, attempts to restore the history file from archive first
3. Parses the history file line by line, extracting timeline IDs and switchpoints
4. Validates the timeline sequence to ensure increasing order
5. Creates an additional "tip" entry for the current timeline
6. Returns a list ordered with newest timeline first

The history file format consists of lines containing: timeline_id, switchpoint_location, and optional comments.

## Parameters / Member Variables
- `targetTLI`: The timeline ID for which to read the history file

## Dependencies
- Functions called/Symbols referenced:
  - [TLHistoryFileName](../T/TLHistoryFileName.md) - constructs timeline history filename
  - [RestoreArchivedFile](../R/RestoreArchivedFile.md) - restores archived file during recovery
  - [TLHistoryFilePath](../T/TLHistoryFilePath.md) - constructs local path to history file
  - [AllocateFile](../A/AllocateFile.md) - opens the history file for reading
  - [FreeFile](../F/FreeFile.md) - closes the file handle
  - [palloc](../p/palloc.md) - allocates memory for timeline entries
  - [lcons](../l/lcons.md) - prepends entries to the result list
  - [KeepFileRestoredFromArchive](../K/KeepFileRestoredFromArchive.md) - preserves restored archive file
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md)/end - reports wait events for monitoring
- Called from (representative examples):
  - [WaitForWALToBecomeAvailable](../W/WaitForWALToBecomeAvailable.md) - during WAL recovery
  - [rescanLatestTimeLine](rescanLatestTimeLine.md) - [when](../w/when.md) rescanning timeline information
  - [XLogFileReadAnyTLI](../X/XLogFileReadAnyTLI.md) - [when](../w/when.md) reading WAL files across timelines
  - [StartReplication](../S/StartReplication.md) - during replication setup
  - various backup and WAL summarizer functions

## Notes and Other Information
- Timeline 1 (master timeline) has no history file and is handled specially
- The function validates timeline ID ordering to detect corrupted history files  
- Switchpoints are stored as 64-bit LSN values constructed from high/low 32-bit parts
- The result list is built with newest timeline entries first
- Comments and empty lines in history files are ignored during parsing
- Located in src/backend/access/transam/timeline.c:76-221