# do_pg_backup_stop

## Location
[src/backend/access/transam/xlog.c:9136-9409](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L9136-L9409)

## Overview
Utility function called at the end of an online backup to finalize the backup process, create backup history files, reset session state, and optionally wait for WAL segments to be archived.

## Definition


## Detailed Description
This function completes an online backup operation by performing several critical tasks:

1. **Backup validation**: Verifies WAL level sufficiency and checks for standby promotion during backup
2. **State management**: Updates running backup counters and resets session-level backup state
3. **End-of-backup record**: Writes XLOG_BACKUP_END record (except during recovery)
4. **History file creation**: Creates backup history files for backup tracking and debugging
5. **WAL archiving**: Optionally waits for required WAL segments to be archived

The function handles different behavior depending on whether the backup is taken during recovery (on a standby) or normal operation (on a primary). During recovery, it uses the minimum recovery point from pg_control as the backup end location instead of writing an end-of-backup record.

## Parameters / Member Variables
- : BackupState structure containing backup information including start point, timeline, and recovery status that gets filled with stop point information
- : Boolean flag indicating whether to wait for WAL segments to be archived before returning

## Dependencies
- Functions called/Symbols referenced:
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - XLogIsNeeded
  - [WALInsertLockAcquireExclusive](../W/WALInsertLockAcquireExclusive.md)/WALInsertLockRelease
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogInsert](../X/XLogInsert.md)
  - [RequestXLogSwitch](../R/RequestXLogSwitch.md)
  - BackupHistoryFilePath
  - [build_backup_content](../b/build_backup_content.md)
  - CleanupBackupHistory
  - XLogArchivingActive/XLogArchivingAlways
  - [XLogArchiveIsBusy](../X/XLogArchiveIsBusy.md)
- Called from:
  - [perform_base_backup](../p/perform_base_backup.md) (src/backend/backup/basebackup.c:394)
  - PG_BACKUP_STOP_V2_COLS function implementations

## Notes and Other Information
- Must be matched with exactly one do_pg_backup_start() call
- The function expects the caller to verify user permissions
- During recovery, backup history files are not created as they won't be archived
- WAL archiving wait loop continues indefinitely until segments are archived or interrupted
- Uses exclusive WAL insertion lock to maintain consistency when updating backup counters
- File location: src/backend/access/transam/xlog.c:9136-9409