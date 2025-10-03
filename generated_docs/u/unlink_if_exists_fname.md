# unlink_if_exists_fname

## Location
[src/backend/storage/file/fd.c:3769-3793](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L3769-L3793)

## Overview
A callback function that removes files and directories, with different handling strategies for regular files versus directories and built-in tolerance for already-deleted items.

## Definition

```c
static void
unlink_if_exists_fname(const char *fname, bool isdir, int elevel)
```
## Detailed Description
unlink_if_exists_fname is a callback function designed to be used with walkdir() for recursive deletion of directory trees. It handles both regular files and directories using appropriate removal strategies for each type.

The function implements different deletion approaches based on the file type:
- For directories: Uses rmdir() directly with error handling that ignores ENOENT (file not found) errors
- For regular files: Uses PathNameDeleteTemporaryFile() which provides additional functionality like file size reporting and proper temporary file cleanup

The function is designed to be fault-tolerant, specifically ignoring cases where files or directories have already been deleted (ENOENT errors). This makes it suitable for cleanup operations where concurrent processes might be modifying the file system or where the cleanup operation might be run multiple times.

Key characteristics:
- Error-tolerant design that handles already-deleted items gracefully
- Different strategies for files vs directories to optimize cleanup
- Integrates with PostgreSQL's temporary file management system
- Suitable for use in cleanup and recovery scenarios

## Parameters / Member Variables
- `*fname`: Full path to the file or directory to be removed
- `isdir`: Boolean flag indicating whether the path is a directory
- `elevel`: Error reporting level for logging deletion issues
## Dependencies
- Functions called/Symbols referenced:
  - rmdir: System call to remove empty directories
  - [PathNameDeleteTemporaryFile](../P/PathNameDeleteTemporaryFile.md): PostgreSQL function for file deletion with additional reporting
  - [errcode_for_file_access](../e/errcode_for_file_access.md): Error code generation for file access errors
  - ereport: Error reporting function
- Called from (representative examples):
  - [PathNameDeleteTemporaryDir](../P/PathNameDeleteTemporaryDir.md): During temporary directory cleanup operations

## Notes and Other Information
- Part of PostgreSQL's temporary file and directory management system
- The ENOENT tolerance makes it suitable for cleanup operations that might be run multiple times
- Uses PathNameDeleteTemporaryFile for regular files to get better error reporting and file size tracking
- Directories must be empty for rmdir() to succeed, which works well with walkdir's post-order traversal
- Error handling is configurable through the elevel parameter, allowing different cleanup contexts to use different error reporting strategies
- Designed to work with the walkdir recursive traversal pattern where directories are processed after their contents