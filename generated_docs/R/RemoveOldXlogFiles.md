# RemoveOldXlogFiles

## Location
src/backend/access/transam/xlog.c: 3842 - 3916

## Overview
Recycles or removes WAL log files that are older than or equal to a specified segment number, helping manage disk space while ensuring required WAL segments for recovery are preserved.

## Definition


## Detailed Description
This function manages the lifecycle of WAL files by removing or recycling segments that are no longer needed for recovery. It intelligently decides between recycling (reusing) and deletion based on various factors including the current WAL position and the last checkpoint's redo pointer. 

The function operates by:
1. Calculating recycling boundaries using the end pointer and redo pointer
2. Constructing the filename of the last segment to be preserved
3. Scanning the pg_wal directory for eligible files
4. For each WAL segment older than the threshold, checking if it has been archived
5. Updating shared memory tracking before removal
6. Calling RemoveXlogFile to handle the actual recycling or deletion

The algorithm preserves segments from parent timelines to avoid premature removal and uses alphanumeric filename sorting to determine segment age.

## Parameters / Member Variables
- : The segment number threshold - files older than or equal to this will be considered for removal
- : The redo pointer from the last checkpoint, used to determine recycling vs deletion strategy  
- : Current or recent end of WAL, used for recycling calculations
- : The current timeline ID for XLOG insertion - recycled segments will be reused for this timeline

## Dependencies
- Functions called/Symbols referenced:
  - XLByteToSeg
  - XLOGfileslop
  - XLogFileName
  - AllocateDir
  - ReadDir
  - FreeDir
  - IsXLogFileName
  - IsPartialXLogFileName
  - XLogArchiveCheckDone
  - UpdateLastRemovedPtr
  - RemoveXlogFile
  - elog
- Constants used:
  - XLOGDIR
  - DEBUG2
  - MAXFNAMELEN
- Called from:
  - CreateCheckPoint
  - CreateRestartPoint
  - RefreshXLogWriteResult

## Notes and Other Information
- This function is declared as static, meaning it's only accessible within the xlog.c file
- The timeline portion of WAL segment names is ignored during comparison to prevent premature removal of parent timeline segments
- Files are only removed after confirming they have been successfully archived (via XLogArchiveCheckDone)
- The function updates shared memory tracking (via UpdateLastRemovedPtr) before actual file removal
- Alphanumeric sorting of filenames is used to determine segment chronological order
- File path: src/backend/access/transam/xlog.c:3842-3916