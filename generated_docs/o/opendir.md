# opendir

## Location
src/port/dirent.c: 33 - 77

## Overview
PostgreSQL's Windows-specific implementation of the POSIX opendir() function that opens a directory stream for reading directory entries.

## Definition

```c
DIR *
opendir(const char *dirname)
```
## Detailed Description
This function is a Windows compatibility implementation of the POSIX opendir() function, located in src/port/dirent.c. It creates and initializes a DIR structure to represent an opened directory stream that can be used with readdir() and closedir(). The implementation handles Windows-specific directory access using Windows API functions like GetFileAttributes() to validate the directory path.

The function performs several validation steps: first checking if the path exists and is actually a directory, then allocating memory for the DIR structure and preparing it for subsequent directory reading operations. It appends a wildcard pattern ("*") to the directory path to enable Windows FindFirstFile/FindNextFile operations.

## Parameters / Member Variables
- : Path to the directory to be opened for reading

## Dependencies
- Functions called/Symbols referenced:
  - DIR (Windows directory structure type)
  - malloc (memory allocation)
  - DT_UNKNOWN (directory entry type constant)
  - dirent (directory entry structure)

- Called from (representative examples):
  - AllocateDir (src/backend/storage/file/fd.c:2858)
  - CleanupPriorWALFiles (src/bin/pg_archivecleanup/pg_archivecleanup.c:98)
  - scan_directory (src/bin/pg_checksums/pg_checksums.c:308)
  - process_directory_recursively (src/bin/pg_combinebackup/pg_combinebackup.c:932)
  - sync_pgdata (src/common/file_utils.c:149)
  - walkdir (src/common/file_utils.c:278)
  - pgfnames (src/common/pgfnames.c:45)

## Notes and Other Information
- This is a Windows-specific implementation that provides POSIX compatibility
- The function allocates memory that must be freed by calling closedir()
- Returns NULL on error with appropriate errno values (ENOENT, ENOTDIR, ENOMEM)
- Automatically appends backslash and wildcard (*) to directory path for Windows file search operations
- Sets handle to INVALID_HANDLE_VALUE initially, which gets used by subsequent readdir() calls
- Part of PostgreSQL's portability layer for Windows systems