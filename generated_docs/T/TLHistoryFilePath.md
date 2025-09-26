# TLHistoryFilePath

## Location
[src/include/access/xlog_internal.h:232-237](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xlog_internal.h#L232-L237)

## Overview
TLHistoryFilePath is an inline function that constructs the complete file system path for a timeline history file based on a given timeline ID, combining the WAL directory path with the timeline history filename.

## Definition

```c
static inline void
TLHistoryFilePath(char *path, TimeLineID tli)
```
## Detailed Description
This function generates the full file system path for a timeline history file by combining PostgreSQL's WAL directory path (XLOGDIR) with the timeline-specific filename format. The resulting path follows the pattern "pg_wal/TTTTTTTT.history" where TTTTTTTT is the 8-digit hexadecimal representation of the timeline ID. This function is essential for timeline management operations that need to read, write, or check the existence of timeline history files on disk.

Timeline history files contain critical metadata about timeline relationships and are stored alongside WAL segment files in the pg_wal directory.

## Parameters / Member Variables
- : Output buffer that receives the constructed file path (must be at least MAXPGPATH bytes)
- : Timeline ID for which to generate the complete file path

## Dependencies
- Functions called/Symbols referenced:
  - XLOGDIR (macro defining the WAL directory path, typically "pg_wal")
- Called from (representative examples):
  - readTimeLineHistory (reads timeline history from the specified path)
  - writeTimeLineHistory (writes timeline history to the specified path)
  - existsTimeLineHistory (checks if timeline history file exists at the path)
  - writeTimeLineHistoryFile (writes timeline history during recovery)
  - SendTimeLineHistory (accesses timeline history for replication)

## Notes and Other Information
- This is an inline function defined in the header for performance optimization
- The function uses snprintf for safe string formatting with buffer bounds checking
- Timeline history files are essential for maintaining WAL timeline consistency across recovery operations
- The generated path is used by both backend processes and utility programs like pg_rewind
- Timeline history files must be accessible from the same directory as WAL segment files
- This function provides a centralized way to generate consistent paths for timeline history operations
- The path format ensures timeline history files are co-located with their associated WAL segments