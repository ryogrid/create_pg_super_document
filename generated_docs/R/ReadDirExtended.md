# ReadDirExtended

## Location
src/backend/storage/file/fd.c: 2921 - 2957

## Overview
ReadDirExtended provides configurable error-level directory reading functionality, serving as the core implementation for directory traversal operations with flexible error handling.

## Definition
```c
struct dirent *ReadDirExtended(DIR *dir, const char *dirname, int elevel)
```

## Detailed Description
ReadDirExtended is the foundational directory reading function in PostgreSQL's file management system that allows callers to specify the error reporting level. It handles both initial AllocateDir failures and subsequent readdir() failures with configurable error severity. When elevel is set below ERROR, the function returns NULL on any error without aborting the transaction, allowing callers to implement custom error handling or continue processing despite directory read failures.

The function carefully manages errno to distinguish between end-of-directory (errno remains 0) and actual errors (errno is set by readdir). This distinction is crucial for proper directory traversal logic where reaching the end of a directory is expected behavior, not an error condition.

## Parameters / Member Variables
- `dir`: Directory stream pointer returned by AllocateDir, or NULL if AllocateDir failed
- `dirname`: Directory path name used for error reporting purposes
- `elevel`: Error reporting level (ERROR, WARNING, LOG, etc.) - controls how failures are handled

## Dependencies
- Functions called/Symbols referenced:
  - readdir (system call)
  - ereport (PostgreSQL error reporting)
  - errcode_for_file_access (PostgreSQL error code function)
  - DIR (system type)
  - dirent (system structure)
- Called from (representative examples):
  - ReadDir
  - ReorderBufferCleanupSerializedTXNs
  - RemovePgTempFiles
  - RemovePgTempFilesInDir
  - SyncDataDirectory
  - walkdir
  - RelationCacheInitFileRemove
  - DeleteAllExportedSnapshotFiles

## Notes and Other Information
- Returns NULL on error or end of directory
- When elevel < ERROR, allows graceful error handling without transaction abort
- Properly distinguishes between end-of-directory and error conditions using errno
- Provides generic error messages for both AllocateDir and readdir failures
- Core function underlying all PostgreSQL directory reading operations
- Used extensively in cleanup, backup, and maintenance operations
- The dirname parameter must match the original path used with AllocateDir for accurate error reporting