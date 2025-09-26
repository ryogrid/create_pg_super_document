# readdir

## Location
[src/port/dirent.c:78-126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/dirent.c#L78-L126)

## Overview
The readdir function provides a Windows-compatible implementation of the POSIX readdir() function, enabling directory traversal by returning successive directory entries from an opened DIR structure.

## Definition

```c
struct dirent *
readdir(DIR *d)
```
## Detailed Description
This function implements the POSIX readdir() interface for Windows systems using the Windows FindFirstFile/FindNextFile APIs. It maintains directory iteration state through the DIR structure and returns directory entries one at a time. On the first call for a given DIR, it uses FindFirstFile to begin enumeration; subsequent calls use FindNextFile to continue. The function handles Windows-specific file attributes and converts them to POSIX-compatible dirent types, including special handling for reparse points (symbolic links) and distinguishing between directories and regular files.

The function ensures proper errno handling to match POSIX semantics, setting errno to 0 when no more files exist (normal end-of-directory condition) and mapping Windows error codes to appropriate errno values for actual errors.

## Parameters / Member Variables
- `d`: Pointer to a DIR structure containing directory iteration state, including the directory name, Windows handle, and return buffer

## Dependencies
- Functions called/Symbols referenced:
  - DIR (struct)
  - _dosmaperr
  - DT_LNK
  - DT_DIR  
  - DT_REG
- Called from (representative examples):
  - ReadDirExtended
  - CleanupPriorWALFiles
  - FindStreamingStart
  - scan_directory
  - process_directory_recursively
  - pgfnames
  - rmtree

## Notes and Other Information
This is a Windows-specific implementation that bridges POSIX directory reading semantics with Windows file system APIs. The function correctly handles reparse points by checking the ReparseTag field first, as reparse points are also reported as directories by Windows. File type detection maps Windows file attributes to standard POSIX dirent d_type values (DT_REG for regular files, DT_DIR for directories, DT_LNK for symbolic links). The function is extensively used throughout PostgreSQL for directory traversal operations including WAL management, backup processes, cleanup operations, and general file system utilities. Error handling follows POSIX conventions with errno set to 0 for end-of-directory and appropriate error codes for actual failures.