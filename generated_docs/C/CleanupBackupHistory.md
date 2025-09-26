# CleanupBackupHistory

## Location
src/backend/access/transam/xlog.c: 4138 - 4180

## Overview
Removes archived backup history files from the WAL directory after confirming they have been successfully archived, helping manage disk space and maintaining WAL directory cleanliness.

## Definition
static void CleanupBackupHistory(void)

## Detailed Description
CleanupBackupHistory is a static function that scans the WAL directory (XLOGDIR) for backup history files and removes those that have been successfully archived. It iterates through all files in the directory, identifies backup history files using IsBackupHistoryFileName(), and checks their archival status via XLogArchiveCheckDone(). For files that have been archived, it removes both the backup history file itself and any associated archive notification (.ready) files using XLogArchiveCleanup(). This function is essential for maintaining WAL directory hygiene by preventing accumulation of old backup history files while ensuring data integrity by only removing files that have been properly archived.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - AllocateDir
  - ReadDir
  - FreeDir
  - IsBackupHistoryFileName
  - XLogArchiveCheckDone
  - XLogArchiveCleanup
  - unlink
  - elog
  - snprintf
- Called from (representative examples):
  - RefreshXLogWriteResult
  - do_pg_backup_stop

## Notes and Other Information
- This function operates only on backup history files, not regular WAL files
- It uses DEBUG2 level logging when removing backup history files
- The function retries creation of .ready files for backup history files where XLogArchiveNotify failed previously
- It's called during WAL management operations to maintain directory cleanliness
- The function is safe to call repeatedly as it only removes files that have been confirmed as archived