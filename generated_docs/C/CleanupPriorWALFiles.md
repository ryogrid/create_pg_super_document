# CleanupPriorWALFiles

## Location
[src/bin/pg_archivecleanup/pg_archivecleanup.c:91-182](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_archivecleanup/pg_archivecleanup.c#L91-L182)

## Overview
Scans the archive directory and removes WAL files that are older than the specified cleanup threshold, supporting both dry-run mode and actual file deletion.

## Definition
```c
static void CleanupPriorWALFiles(void)
```

## Detailed Description
The CleanupPriorWALFiles function is the core cleanup routine of pg_archivecleanup that performs the actual work of identifying and removing outdated WAL files from the archive directory. It scans through all files in the archive location, identifies valid WAL files (including partial WAL files and optionally backup history files), and removes those that are older than the specified exclusiveCleanupFileName threshold.

The function implements timeline-aware cleanup logic, ignoring timeline prefixes when comparing filenames to ensure parent timeline segments are not prematurely removed. It uses alphanumeric filename sorting to determine file ordering rather than filesystem timestamps. The function supports both dry-run mode (where files are only listed but not deleted) and normal operation mode.

## Parameters / Member Variables
This function takes no parameters and operates on several global variables:
- `archiveLocation`: Directory path containing the WAL archive files
- `exclusiveCleanupFileName`: The oldest WAL filename that should be preserved (files older than this are candidates for removal)
- `additional_ext`: Optional extension to trim from filenames before processing
- `cleanBackupHistory`: Boolean flag indicating whether backup history files should also be cleaned up
- `dryrun`: Boolean flag indicating whether to perform actual deletion or just show what would be deleted

## Dependencies
- Functions called/Symbols referenced:
  - [DIR](../D/DIR.md) (directory stream type)
  - struct dirent (directory entry structure)
  - [opendir](../o/opendir.md) (open directory stream)
  - [readdir](../r/readdir.md) (read directory entries)
  - [strlcpy](../s/strlcpy.md) (safe string copy)
  - [TrimExtension](../T/TrimExtension.md) (custom function to remove file extensions)
  - [IsXLogFileName](../I/IsXLogFileName.md) (PostgreSQL function to validate WAL filenames)
  - [IsPartialXLogFileName](../I/IsPartialXLogFileName.md) (PostgreSQL function to validate partial WAL filenames)
  - [IsBackupHistoryFileName](../I/IsBackupHistoryFileName.md) (PostgreSQL function to validate backup history filenames)
  - pg_log_debug (PostgreSQL logging function)
  - unlink (remove file)
  - [closedir](../c/closedir.md) (close directory stream)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL fatal error logging)
- Called from (representative examples):
  - [main](../m/main.md) (in src/bin/pg_archivecleanup/pg_archivecleanup.c:393)

## Notes and Other Information
- The function is marked as `static`, making it internal to the pg_archivecleanup.c file
- File comparison is performed by comparing characters starting from position 8 in the filename, effectively ignoring the timeline portion
- Files are not removed in the order they were originally written due to the alphanumeric sorting approach
- In dry-run mode, filenames are printed to stdout for potential piping to other programs
- The function handles various error conditions including directory access failures and file removal errors
- Truncation of long filenames is considered harmless as non-WAL files are filtered out anyway
- Located at src/bin/pg_archivecleanup/pg_archivecleanup.c:91-182