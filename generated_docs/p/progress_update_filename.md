# progress_update_filename

## Location
[src/bin/pg_basebackup/pg_basebackup.c:792-815](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_basebackup.c#L792-L815)

## Overview
A callback function used in pg_basebackup to update the global variable tracking the current filename being processed for progress reporting purposes.

## Definition
```c
static void progress_update_filename(const char *filename)
```

## Detailed Description
This static function serves as a callback to update the global `progress_filename` variable that tracks the current file being processed during a base backup operation. It is designed to be used exclusively for progress reporting when both verbose output and progress reporting are enabled. The function ensures proper memory management by freeing any existing filename string before setting a new one.

The function only performs its operations when both `showprogress` and `verbose` flags are true, making it efficient by avoiding unnecessary string operations when detailed progress reporting is not required.

## Parameters / Member Variables
- `filename`: A C string containing the name of the file currently being processed, or NULL to clear the current filename

## Dependencies
- Functions called/Symbols referenced:
  - free (standard C library function)
  - [pg_strdup](pg_strdup.md) (PostgreSQL string duplication utility)
- Called from (representative examples):
  - CompressionLocation
  - [CreateBackupStreamer](../C/CreateBackupStreamer.md)
  - [ReceiveTarFile](../R/ReceiveTarFile.md)
  - [BaseBackup](../B/BaseBackup.md)

## Notes and Other Information
- This is a static function, meaning it has internal linkage and is only accessible within the same source file
- The function includes a comment warning that no other code should modify progress_filename directly
- Memory management is handled carefully: the previous filename string is freed before setting a new one
- The function is conditionally active only when both progress reporting and verbose output are enabled
- Setting filename to NULL will clear the current progress filename