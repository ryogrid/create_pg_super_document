# writeTimeLineHistory

## Location
src/backend/access/transam/timeline.c: 304 - 462

## Overview
Creates a new timeline history file by copying the parent timeline history and appending information about the new timeline split.

## Definition
```c
void writeTimeLineHistory(TimeLineID newTLI, TimeLineID parentTLI, XLogRecPtr switchpoint, char *reason)
```

## Detailed Description
This function creates a timeline history file for a new timeline during operations like standby promotion or timeline switching. The process involves several critical steps:

1. **Temporary file creation**: Creates a temporary file with a unique name to avoid corruption during write
2. **Parent history copying**: If the parent timeline has a history file, copies its entire contents verbatim to preserve the complete timeline ancestry
3. **New entry addition**: Appends a new line containing the parent timeline ID, switchpoint LSN, and human-readable reason for the timeline split
4. **Atomic completion**: Uses durable_rename to atomically move the completed file to its final location
5. **Archive notification**: Notifies the archiver that the new history file is ready for archival

The function includes robust error handling throughout, cleaning up temporary files on failure and providing detailed error messages. It also uses proper wait event reporting for performance monitoring.

## Parameters / Member Variables
- `newTLI`: The timeline ID of the new timeline being created
- `parentTLI`: The timeline ID of the immediate parent timeline
- `switchpoint`: The WAL location (LSN) where the system switched to the new timeline
- `reason`: A human-readable explanation of why the timeline was switched

## Dependencies
- Functions called/Symbols referenced:
  - OpenTransientFile - opens temporary and source files
  - CloseTransientFile - closes file handles
  - TLHistoryFileName - constructs timeline history filename
  - TLHistoryFilePath - constructs local path to history file
  - [RestoreArchivedFile](../R/RestoreArchivedFile.md) - restores parent history from archive if needed
  - [durable_rename](../d/durable_rename.md) - atomically renames temporary file to final location
  - XLogArchivingActive - checks if archiving is enabled
  - [XLogArchiveNotify](../X/XLogArchiveNotify.md) - notifies archiver of new file
  - pg_fsync - ensures data is written to disk
  - pgstat_report_wait_start/end - reports wait events for monitoring
  - Various system calls: read, write, unlink, access
- Called from (representative examples):
  - [StartupXLOG](../S/StartupXLOG.md) - during database startup when creating new timelines

## Notes and Other Information
- Used primarily at the end of recovery operations when promoting a standby or switching timelines
- The function ensures atomic file creation using temporary files and durable_rename
- Parent timeline history is preserved by copying the entire parent history file
- Includes comprehensive error handling with cleanup of temporary files on failure
- Timeline history files are immediately eligible for archival upon creation
- The new timeline ID must be greater than the parent timeline ID (enforced by assertion)
- Located in src/backend/access/transam/timeline.c:304-462