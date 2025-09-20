# lstat_error_was_status_delete_pending

## Location
[src/port/dirmod.c:104-118](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/dirmod.c#L104-L118)

## Overview
A Windows-specific helper function that determines if an lstat operation failed due to a STATUS_DELETE_PENDING condition, indicating a file is marked for deletion but still exists.

## Definition

```c
struct stat st;
```
## Detailed Description
This static function serves as a diagnostic utility to identify a specific Windows file system condition where a file has been marked for deletion but still physically exists on disk. On Windows, when a file is deleted while still open by another process, the system marks it with STATUS_DELETE_PENDING status rather than immediately removing it. This function checks if a previous  call failed specifically due to this condition.

The function first checks if the current errno is ENOENT (file not found), and if so, it examines the underlying NT status code to determine if the actual cause was STATUS_DELETE_PENDING. This distinction is important for proper error handling in file operations, as it allows PostgreSQL to differentiate between truly missing files and files that are in a pending deletion state.

## Parameters / Member Variables
This function takes no parameters and returns a boolean value.

## Dependencies
- Functions called/Symbols referenced:
  -  (Windows-specific NT status retrieval function)
- Called from (representative examples):
  -  (file unlinking function in dirmod.c)

## Notes and Other Information
- This function is marked as , making it internal to the dirmod.c compilation unit
- The function is Windows-specific and does not apply to Cygwin, which uses its own lstat() implementation that reports STATUS_DELETE_PENDING as EACCES
- Returns  if the last lstat error was specifically due to STATUS_DELETE_PENDING,  otherwise
- The function relies on the global  variable being set by a previous system call
- This is part of PostgreSQL's cross-platform file handling infrastructure, addressing Windows-specific file system semantics
- The STATUS_DELETE_PENDING condition is a Windows NTFS feature that allows files to persist until all handles are closed