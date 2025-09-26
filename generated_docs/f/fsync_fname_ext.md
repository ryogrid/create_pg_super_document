# fsync_fname_ext

## Location
src/backend/storage/file/fd.c: 3794 - 3869

## Overview
A comprehensive file/directory synchronization function that safely fsyncs files or directories with appropriate error handling for platform-specific behaviors.

## Definition
```c
int fsync_fname_ext(const char *fname, bool isdir, bool ignore_perm, int elevel)
```

## Detailed Description
This function provides a robust wrapper around fsync operations that handles the cross-platform differences in file and directory synchronization. It opens the specified file or directory with appropriate flags, performs the fsync operation, and handles various platform-specific error conditions gracefully. The function is designed to be tolerant of permission errors when requested and logs errors at a caller-specified level.

The function handles several OS-specific behaviors:
- Some OSes require directories to be opened read-only while others don't allow fsync on read-only files
- Windows returns EACCES when trying to open directories
- Some systems don't allow fsync on directories and return EBADF or EINVAL

## Parameters / Member Variables
- `fname`: The path to the file or directory to be synced
- `isdir`: Boolean flag indicating whether the target is a directory (true) or file (false)
- `ignore_perm`: Boolean flag to ignore permission errors (EACCES) when opening files
- `elevel`: Error logging level to use for reporting errors (e.g., ERROR, WARNING)

## Dependencies
- Functions called/Symbols referenced:
  - OpenTransientFile: Opens the file/directory with transient file management
  - pg_fsync: PostgreSQL's fsync wrapper function
  - CloseTransientFile: Closes the transient file descriptor
  - PG_BINARY: Binary file flag for cross-platform compatibility
  - ereport: Error reporting function
  - errcode_for_file_access: Error code generation for file access errors

- Called from (representative examples):
  - fsync_fname: Simpler fsync wrapper function
  - durable_rename: File rename with durability guarantees
  - datadir_fsync_fname: Data directory specific fsync
  - fsync_parent_path: Parent directory synchronization

## Notes and Other Information
- Returns 0 on success, -1 on failure
- Handles platform-specific directory fsync limitations gracefully
- Uses transient file descriptors to avoid file descriptor leaks
- Provides flexible error reporting through configurable error levels
- Critical for ensuring data durability in PostgreSQL's storage operations