# AddToDataDirLockFile

## Location
src/backend/utils/init/miscinit.c: 1566 - 1692

## Overview
Adds or replaces a specific line in the data directory lock file with atomic write operations to maintain consistency.

## Definition
```c
void AddToDataDirLockFile(int target_line, const char *str)
```

## Detailed Description
This function performs an atomic update of a specific line in the data directory lock file (typically "postmaster.pid"). It reads the entire lock file into memory, modifies the specified line with the given string, and writes the entire content back in a single operation. The function ensures atomicity by performing the write in one kernel call and includes proper error handling and wait event reporting for monitoring. It handles cases where lines are added out of order by filling in missing lines with newlines. The implementation intentionally avoids truncating the file to maintain atomic updates, which means callers should avoid shortening lines once written.

## Parameters / Member Variables
- `target_line`: The line number (1-based) in the lock file to add or replace
- `str`: The string content to write to the specified line (should not include trailing newline)

## Dependencies
- Functions called/Symbols referenced:
  - open
  - read
  - pg_pwrite
  - pg_fsync
  - close
  - pgstat_report_wait_start
  - pgstat_report_wait_end
  - DIRECTORY_LOCK_FILE (constant)
  - PG_BINARY (constant)
- Called from (representative examples):
  - InternalIpcMemoryCreate
  - PostmasterMain
  - process_pm_shutdown_request
  - process_pm_child_exit
  - process_pm_pmsignal

## Notes and Other Information
- Updates are atomic due to single kernel call write operation
- File is not truncated to maintain atomicity, so callers should avoid shortening lines
- Includes comprehensive error handling with appropriate logging
- Uses wait events for monitoring I/O operations
- Handles out-of-order line additions by filling gaps with newlines
- Critical for maintaining lock file consistency during server state changes