# FindEndOfXLOG

## Location
[src/bin/pg_resetwal/pg_resetwal.c:907-972](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_resetwal/pg_resetwal.c#L907-L972)

## Overview
FindEndOfXLOG scans existing WAL files to determine the highest WAL address in use and calculates a safe starting point for new WAL segments after a reset operation.

## Definition
```c
static void FindEndOfXLOG(void)
```

## Detailed Description
This static function is a critical component of pg_resetwal that analyzes the existing WAL (Write-Ahead Log) directory to determine where new WAL segments should begin. The function performs a comprehensive scan of the pg_wal directory to find all existing WAL segment files and determines the highest segment number in use.

The function operates in several phases:
1. Initializes the search using the last checkpoint address from the control file
2. Scans the pg_wal directory for existing WAL segment files (both complete and partial)
3. Extracts segment numbers from all found WAL files, taking the maximum across all timelines
4. Converts the result to the new WAL segment size format
5. Advances by one segment to ensure the new WAL starts in "virgin territory"

The function is conservative in its approach - it considers files from all timelines and errs on the side of choosing a higher segment number rather than risking overlap with existing data.

## Parameters / Member Variables
This function takes no parameters and operates on:
- Global ControlFile structure (reads checkpoint redo position and segment size)
- Global newXlogSegNo variable (sets the calculated new segment number)

## Dependencies
- Functions called/Symbols referenced:
  - XLByteToSeg (converts byte position to segment number)
  - [opendir](../o/opendir.md), readdir, closedir (POSIX directory operations)
  - [IsXLogFileName](../I/IsXLogFileName.md) (checks if filename is a WAL segment)
  - [IsPartialXLogFileName](../I/IsPartialXLogFileName.md) (checks if filename is a partial WAL segment)
  - [XLogFromFileName](../X/XLogFromFileName.md) (extracts timeline and segment number from filename)
  - XLOGDIR (constant for WAL directory path)
  - [DIR](../D/DIR.md), dirent (POSIX directory structures)
  - TimeLineID, XLogSegNo (PostgreSQL WAL types)

- Called from:
  - [main](../m/main.md) (in pg_resetwal.c at line 400)

## Notes and Other Information
- This is a static function local to pg_resetwal.c
- The function is conservative and considers WAL files from all timelines, not just the target timeline
- It handles both complete and partial WAL segment files
- The function accounts for potential differences between old and new WAL segment sizes
- Error handling includes checks for directory operations (open, read, close)
- The final "+1" ensures that the new WAL starts in completely unused space
- Critical for ensuring data integrity during WAL reset operations by avoiding overlap with existing WAL data
- The function assumes that any present WAL files have been used, following xlog.c's file pre-creation behavior