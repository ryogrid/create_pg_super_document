# KillExistingXLOG

## Location
src/bin/pg_resetwal/pg_resetwal.c: 973 - 1005

## Overview
KillExistingXLOG removes all existing WAL (Write-Ahead Log) files from the pg_wal directory as part of the database reset operation.

## Definition
```c
static void KillExistingXLOG(void)
```

## Detailed Description
This static function is a destructive operation within the pg_resetwal utility that systematically removes all existing WAL segment files from the pg_wal directory. This function is called after the decision has been made to reset the WAL and all necessary precautions and confirmations have been completed.

The function operates by:
1. Opening the WAL directory (pg_wal)
2. Iterating through all directory entries
3. Identifying WAL segment files (both complete and partial) using PostgreSQL's filename conventions
4. Deleting each identified WAL file using the unlink system call
5. Performing proper error handling for all directory and file operations

This is a critical and irreversible operation that permanently removes WAL data, making it impossible to perform point-in-time recovery to any point before the reset operation.

## Parameters / Member Variables
This function takes no parameters and operates directly on the filesystem in the pg_wal directory.

## Dependencies
- Functions called/Symbols referenced:
  - opendir, readdir, closedir (POSIX directory operations)
  - IsXLogFileName (checks if filename matches WAL segment pattern)
  - IsPartialXLogFileName (checks if filename matches partial WAL segment pattern)
  - snprintf (formats the full file path)
  - unlink (POSIX function to delete files)
  - XLOGDIR (constant for WAL directory path)
  - MAXPGPATH (constant for maximum PostgreSQL path length)
  - DIR, dirent (POSIX directory structures)

- Called from:
  - main (in pg_resetwal.c at line 495)

## Notes and Other Information
- This is a static function local to pg_resetwal.c
- This function is DESTRUCTIVE and irreversibly removes WAL data
- Called only after all validations and user confirmations are complete
- Handles both complete WAL segment files and partial WAL segment files
- Includes comprehensive error checking for directory operations and file deletion
- The function ensures that the pg_wal directory is left empty of WAL files
- Critical for ensuring a clean slate before writing the new empty WAL segment
- Should only be called as part of a controlled database reset operation
- Once this function completes successfully, point-in-time recovery to states before the reset becomes impossible
- Part of the irreversible "point of no return" operations in pg_resetwal