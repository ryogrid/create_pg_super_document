# do_pg_backup_stop

## Location
[src/backend/access/transam/xlog.c:9136-9409](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L9136-L9409)

## Overview
Utility function called at the end of an online backup to finalize the backup process, create backup history files, reset session state, and optionally wait for WAL segments to be archived.

## Definition

```c
void
do_pg_backup_stop(BackupState *state, bool waitforarchive)
```
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
  - [BackupHistoryFilePath](../B/BackupHistoryFilePath.md)
  - [build_backup_content](../b/build_backup_content.md)
  - [CleanupBackupHistory](../C/CleanupBackupHistory.md)
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

## Simplified Source

```c
// Simplified version of do_pg_backup_stop (complex function condensed)
void do_pg_backup_stop(BackupState *state, bool waitforarchive) {
    bool backup_stopped_in_recovery = RecoveryInProgress();

    // Validate WAL level
    if (!backup_stopped_in_recovery && !XLogIsNeeded())
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("WAL level not sufficient for making an online backup")));

    // Update backup counters and session state
    WALInsertLockAcquireExclusive();
    Assert(XLogCtl->Insert.runningBackups > 0);
    XLogCtl->Insert.runningBackups--;
    sessionBackupState = SESSION_BACKUP_NONE;
    WALInsertLockRelease();

    // Check for standby promotion during backup
    if (state->started_in_recovery && !backup_stopped_in_recovery)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("the standby was promoted during online backup")));

    if (backup_stopped_in_recovery) {
        // Recovery mode: validate full-page writes and use min recovery point
        SpinLockAcquire(&XLogCtl->info_lck);
        XLogRecPtr recptr = XLogCtl->lastFpwDisableRecPtr;
        SpinLockRelease(&XLogCtl->info_lck);

        if (state->startpoint <= recptr)
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("WAL generated with \"full_page_writes=off\" was replayed")));

        LWLockAcquire(ControlFileLock, LW_SHARED);
        state->stoppoint = ControlFile->minRecoveryPoint;
        state->stoptli = ControlFile->minRecoveryPointTLI;
        LWLockRelease(ControlFileLock);
    } else {
        // Normal mode: write end-of-backup record and create history file
        XLogBeginInsert();
        XLogRegisterData((char *) (&state->startpoint), sizeof(state->startpoint));
        state->stoppoint = XLogInsert(RM_XLOG_ID, XLOG_BACKUP_END);
        state->stoptli = XLogCtl->InsertTimeLineID;

        // Force WAL switch
        RequestXLogSwitch(false);
        state->stoptime = (pg_time_t) time(NULL);

        // Create backup history file
        char histfilepath[MAXPGPATH];
        XLogSegNo _logSegNo;
        XLByteToSeg(state->startpoint, _logSegNo, wal_segment_size);
        BackupHistoryFilePath(histfilepath, state->stoptli, _logSegNo,
                              state->startpoint, wal_segment_size);

        FILE *fp = AllocateFile(histfilepath, "w");
        if (!fp)
            ereport(ERROR, (errcode_for_file_access(),
                    errmsg("could not create file \"%s\": %m", histfilepath)));

        char *history_file = build_backup_content(state, true);
        fprintf(fp, "%s", history_file);
        pfree(history_file);

        if (fflush(fp) || ferror(fp) || FreeFile(fp))
            ereport(ERROR, (errcode_for_file_access(),
                    errmsg("could not write file \"%s\": %m", histfilepath)));

        CleanupBackupHistory();
    }

    // Wait for WAL archiving if requested
    if (waitforarchive && ((backup_stopped_in_recovery && XLogArchivingAlways()) ||
                          (!backup_stopped_in_recovery && XLogArchivingActive()))) {
        // Wait for required WAL segments to be archived
        // [Archiving wait loop condensed for brevity]
        char lastxlogfilename[MAXFNAMELEN];
        char histfilename[MAXFNAMELEN];
        int waits = 0;

        while (XLogArchiveIsBusy(lastxlogfilename) || XLogArchiveIsBusy(histfilename)) {
            CHECK_FOR_INTERRUPTS();
            WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                      1000L, WAIT_EVENT_BACKUP_WAIT_WAL_ARCHIVE);
            ResetLatch(MyLatch);
            waits++;
            // Progress reporting logic...
        }
        ereport(NOTICE, (errmsg("all required WAL segments have been archived")));
    }
}
```

Key simplifications made:
- Condensed the WAL archiving wait loop while preserving structure
- Maintained critical backup state management and validation
- Preserved the dual-mode operation (recovery vs normal)
- Kept essential error handling and file operations
- Maintained proper locking patterns and cleanup
- Note: Some detailed logic condensed for brevity but core functionality preserved