# open_target_file

## Location
src/bin/pg_rewind/file_ops.c: 47 - 74

## Overview
Opens a target file for writing in pg_rewind's file operations, with optional truncation and dry-run support.

## Definition


## Detailed Description
This function opens a target file for writing as part of pg_rewind's file synchronization process. It manages a single global file descriptor (dstfd) and ensures only one target file is open at a time. The function respects the dry_run mode, performing no actual file operations when dry_run is enabled. It constructs the full target path by combining the datadir_target with the relative path provided. If the same file is already open and truncation is not requested, the function returns early without reopening. Otherwise, it closes any currently open file before opening the new one.

## Parameters / Member Variables
- : Relative path of the file to open within the target data directory
- : Boolean flag indicating whether to truncate the file if it already exists (adds O_TRUNC flag)

## Dependencies
- Functions called/Symbols referenced:
  - close_target_file
  - open (system call)
  - snprintf
  - strcmp
  - strlen
  - pg_fatal
- Global variables used:
  - dry_run (configuration flag)
  - dstfd (static file descriptor)
  - dstpath (static path buffer)
  - datadir_target (target directory path)
  - pg_file_create_mode (file permission mode)
- Called from (representative examples):
  - libpq_queue_fetch_file
  - process_queued_fetch_requests
  - local_queue_fetch_file
  - local_queue_fetch_range
  - createBackupLabel

## Notes and Other Information
- Part of pg_rewind utility's file operations module (src/bin/pg_rewind/file_ops.c)
- Maintains a single open file descriptor to avoid resource leaks
- Uses PG_BINARY flag for cross-platform binary file handling
- Honors PostgreSQL's standard file creation permissions via pg_file_create_mode
- Essential for pg_rewind's ability to synchronize files between PostgreSQL data directories
- File operations are designed to be atomic and safe for database file handling