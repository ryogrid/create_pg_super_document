# BufFileTruncateFileSet

## Location
src/backend/storage/file/buffile.c: 933 - 1021

## Overview
Truncates a fileset-based BufFile to a specified file number and offset, removing excess segment files and adjusting the current buffer state accordingly.

## Definition
```c
void BufFileTruncateFileSet(BufFile *file, int fileno, off_t offset)
```

## Detailed Description
BufFileTruncateFileSet performs a comprehensive truncation operation on a fileset-based BufFile by removing all segment files beyond the specified file number and truncating the target file to the specified offset. The function handles several cases: it deletes files with numbers greater than fileno, removes the target file if offset is 0 (except for file 0), and truncates the target file to the specified offset. After physical file operations, it intelligently adjusts the current buffer state based on where the truncation point falls relative to the current buffer position. This ensures the BufFile remains in a consistent state after truncation.

## Parameters / Member Variables
- `file`: Pointer to the BufFile structure to truncate (must be fileset-based)
- `fileno`: The target file number within the fileset where truncation should occur
- `offset`: The byte offset within the target file where truncation should occur

## Dependencies
- Functions called/Symbols referenced:
  - FileSetSegmentName
  - FileClose
  - FileSetDelete
  - FileTruncate
  - FilePathName
  - MAX_PHYSICAL_FILESIZE (constant)
  - WAIT_EVENT_BUFFILE_TRUNCATE
  - ereport/ERROR
- Called from (representative examples):
  - stream_abort_internal (src/backend/replication/logical/worker.c:1795)

## Notes and Other Information
- Only works with fileset-based BufFiles created by BufFileCreateFileSet
- Removes files with numbers greater than the specified fileno
- If offset is 0, removes the target file unless it is file 0 (which gets truncated instead)
- Intelligently adjusts current buffer position and size based on truncation point location
- Reports errors via ereport() if file operations fail
- Used primarily by logical replication worker processes for stream abort operations
- The function maintains buffer consistency by handling three cases: truncation within current buffer, truncation before current position in same file, and truncation in earlier files
- Physical file operations are performed before buffer state adjustments to ensure atomicity