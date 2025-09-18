# remove_target_file

## Location
src/bin/pg_rewind/file_ops.c: 187 - 205

## Overview
Removes a file from the target data directory during PostgreSQL rewind operations, with optional handling for missing files.

## Definition


## Detailed Description
This function is part of the pg_rewind utility's file operations module. It safely removes a specified file from the target PostgreSQL data directory by constructing the full path and calling the system's unlink() function. The function includes error handling that can optionally ignore missing files based on the missing_ok parameter. If dry_run mode is enabled, the function returns early without performing any actual file operations.

## Parameters / Member Variables
- : Relative path to the file within the target data directory that should be removed
- : Boolean flag indicating whether it's acceptable for the target file to not exist (true = ignore missing files, false = treat missing files as an error)

## Dependencies
- Functions called/Symbols referenced:
  - unlink (system call)
  - snprintf (standard library)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL error handling)
- Called from (representative examples):
  - [remove_target](remove_target.md) (src/bin/pg_rewind/file_ops.c:142)
  - [process_queued_fetch_requests](../p/process_queued_fetch_requests.md) (src/bin/pg_rewind/libpq_source.c:567)

## Notes and Other Information
- The function respects the global dry_run flag and performs no operations when dry_run is enabled
- Full target path is constructed by concatenating datadir_target with the provided relative path
- Uses MAXPGPATH constant to ensure path buffer safety
- Error handling distinguishes between ENOENT (file not found) and other system errors
- Part of the pg_rewind utility which synchronizes a PostgreSQL data directory with another one