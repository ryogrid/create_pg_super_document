# RemoveWalSummaryIfOlderThan

## Location
src/backend/backup/walsummary.c: 230 - 262

## Overview
Conditionally removes a WAL summary file from the filesystem if its modification time is older than a specified cutoff time.

## Definition
```c
void RemoveWalSummaryIfOlderThan(WalSummaryFile *ws, time_t cutoff_time)
```

## Detailed Description
RemoveWalSummaryIfOlderThan performs age-based cleanup of WAL summary files by checking the modification time of a specified summary file and removing it if it predates the cutoff time. The function constructs the file path using the same naming convention as other WAL summary functions, then uses lstat() to check the file's modification time. If the file is older than the cutoff, it is removed using unlink(). The function handles missing files gracefully by returning without error if the file doesn't exist.

This function is typically used as part of WAL summary maintenance operations to prevent accumulation of obsolete summary files that are no longer needed for backup or recovery operations.

## Parameters / Member Variables
- `ws`: WalSummaryFile structure containing timeline ID, start LSN, and end LSN for path construction
- `cutoff_time`: Threshold time - files modified before this time will be removed

## Dependencies
- Functions called/Symbols referenced:
  - snprintf
  - lstat
  - unlink
  - ereport
  - errcode_for_file_access
  - errmsg
  - errmsg_internal
  - LSN_FORMAT_ARGS
- Called from (representative examples):
  - MaybeRemoveOldWalSummaries

## Notes and Other Information
- Returns void - no return value
- Silently handles missing files (ENOENT)
- Logs file removal at DEBUG2 level
- Uses standard PostgreSQL error reporting for filesystem errors
- Part of WAL summary maintenance and cleanup infrastructure
- Located in src/backend/backup/walsummary.c:230-262