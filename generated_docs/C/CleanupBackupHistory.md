# CleanupBackupHistory

## Location
[src/backend/access/transam/xlog.c:4138-4180](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L4138-L4180)

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
  - [AllocateDir](../A/AllocateDir.md)
  - [ReadDir](../R/ReadDir.md)
  - [FreeDir](../F/FreeDir.md)
  - [IsBackupHistoryFileName](../I/IsBackupHistoryFileName.md)
  - [XLogArchiveCheckDone](../X/XLogArchiveCheckDone.md)
  - [XLogArchiveCleanup](../X/XLogArchiveCleanup.md)
  - unlink
  - elog
  - snprintf
- Called from (representative examples):
  - RefreshXLogWriteResult
  - [do_pg_backup_stop](../d/do_pg_backup_stop.md)

## Notes and Other Information
- This function operates only on backup history files, not regular WAL files
- It uses DEBUG2 level logging when removing backup history files
- The function retries creation of .ready files for backup history files where XLogArchiveNotify failed previously
- It's called during WAL management operations to maintain directory cleanliness
- The function is safe to call repeatedly as it only removes files that have been confirmed as archived

## Simplified Source

```c
// Remove archived backup history files from WAL directory
static void CleanupBackupHistory(void)
{
    DIR *xldir;
    struct dirent *xlde;
    char path[MAXPGPATH + sizeof(XLOGDIR)];

    // Open the WAL directory
    xldir = AllocateDir(XLOGDIR);

    // Scan all files in the directory
    while ((xlde = ReadDir(xldir, XLOGDIR)) != NULL) {
        // Check if this is a backup history file
        if (IsBackupHistoryFileName(xlde->d_name)) {
            // Check if file has been successfully archived
            if (XLogArchiveCheckDone(xlde->d_name)) {
                elog(DEBUG2, "removing WAL backup history file \"%s\"",
                     xlde->d_name);

                // Build full path and remove the file
                snprintf(path, sizeof(path), XLOGDIR "/%s", xlde->d_name);
                unlink(path);

                // Clean up archive notification files
                XLogArchiveCleanup(xlde->d_name);
            }
        }
    }

    FreeDir(xldir);
}
```