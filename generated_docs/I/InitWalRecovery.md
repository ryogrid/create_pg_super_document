# InitWalRecovery

## Location
[src/backend/access/transam/xlogrecovery.c:512-1026](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L512-L1026)

## Overview
Prepares and initializes the PostgreSQL system for WAL (Write-Ahead Log) recovery, analyzing control files and backup labels to determine the appropriate recovery strategy and starting point.

## Definition
```c
void InitWalRecovery(ControlFileData *ControlFile, bool *wasShutdown_ptr,
                    bool *haveBackupLabel_ptr, bool *haveTblspcMap_ptr)
```

## Detailed Description
InitWalRecovery is a comprehensive function that coordinates the initialization phase of WAL recovery in PostgreSQL. It analyzes the database control file and backup label file (if present) to determine whether crash recovery or archive recovery is needed, and establishes the starting point for recovery operations. The function handles multiple recovery scenarios including normal crash recovery, point-in-time recovery, and standby mode initialization. It sets up the WAL reading infrastructure, manages tablespace mappings, validates recovery parameters, and configures various global recovery state variables. The function does not modify on-disk state except for creating tablespace symlinks and fetching necessary WAL files.

## Parameters / Member Variables
- `ControlFile`: Pointer to the control file data structure containing database state information
- `wasShutdown_ptr`: Output parameter indicating whether the last shutdown was clean
- `haveBackupLabel_ptr`: Output parameter indicating whether a backup_label file was found
- `haveTblspcMap_ptr`: Output parameter indicating whether a tablespace_map file was processed

## Dependencies
- Functions called/Symbols referenced:
  - [readRecoverySignalFile](../r/readRecoverySignalFile.md) (reads recovery signal files)
  - [validateRecoveryParameters](../v/validateRecoveryParameters.md) (validates recovery configuration)
  - [XLogReaderAllocate](../X/XLogReaderAllocate.md) (creates WAL reader)
  - [XLogPrefetcherAllocate](../X/XLogPrefetcherAllocate.md) (creates WAL prefetcher)
  - [read_backup_label](../r/read_backup_label.md) (processes backup label file)
  - [ReadCheckpointRecord](../R/ReadCheckpointRecord.md) (reads checkpoint records)
  - [EnableStandbyMode](../E/EnableStandbyMode.md) (enables standby mode when required)
  - [OwnLatch](../O/OwnLatch.md) (takes ownership of recovery wakeup latch)
  - [tliOfPointInHistory](../t/tliOfPointInHistory.md) (validates timeline consistency)
  - [read_tablespace_map](../r/read_tablespace_map.md) (processes tablespace mappings)
- Called from (representative examples):
  - [StartupXLOG](../S/StartupXLOG.md) (main startup sequence coordinator)

## Notes and Other Information
- Central function in PostgreSQL's recovery initialization process
- Handles both crash recovery and archive recovery scenarios
- Sets up critical global variables like InRecovery, InArchiveRecovery, and StandbyMode
- Creates necessary infrastructure for WAL reading and prefetching
- Validates timeline consistency and recovery parameters
- Manages backup label and tablespace map file processing
- Located in src/backend/access/transam/xlogrecovery.c:512-1026
- Coordinates with StartupXLOG for complete startup sequence

## Simplified Source

```c
// Simplified version of InitWalRecovery
void InitWalRecovery(ControlFileData *ControlFile, bool *wasShutdown_ptr,
                    bool *haveBackupLabel_ptr, bool *haveTblspcMap_ptr) {
    XLogPageReadPrivate *private;
    bool wasShutdown;
    XLogRecord *record;
    DBState dbstate_at_startup;
    bool haveTblspcMap = false;
    bool haveBackupLabel = false;
    CheckPoint checkPoint;
    bool backupFromStandby = false;

    // Initialize recovery target timeline from control file
    dbstate_at_startup = ControlFile->state;
    if (ControlFile->minRecoveryPointTLI > ControlFile->checkPointCopy.ThisTimeLineID)
        recoveryTargetTLI = ControlFile->minRecoveryPointTLI;
    else
        recoveryTargetTLI = ControlFile->checkPointCopy.ThisTimeLineID;

    // Check for recovery signal files and validate parameters
    readRecoverySignalFile();
    validateRecoveryParameters();

    // Take ownership of recovery wakeup latch if needed
    if (ArchiveRecoveryRequested)
        OwnLatch(&XLogRecoveryCtl->recoveryWakeupLatch);

    // Set up WAL reading infrastructure
    private = palloc0(sizeof(XLogPageReadPrivate));
    xlogreader = XLogReaderAllocate(wal_segment_size, NULL,
                                   XL_ROUTINE(.page_read = &XLogPageRead,
                                             .segment_open = NULL,
                                             .segment_close = wal_segment_close),
                                   private);
    if (!xlogreader)
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
                       errmsg("out of memory")));

    xlogreader->system_identifier = ControlFile->system_identifier;
    XLogReaderSetDecodeBuffer(xlogreader, NULL, wal_decode_buffer_size);
    xlogprefetcher = XLogPrefetcherAllocate(xlogreader);

    // Allocate page buffers for WAL consistency checks
    replay_image_masked = (char *) palloc(BLCKSZ);
    primary_image_masked = (char *) palloc(BLCKSZ);

    // Process backup_label file if present
    if (read_backup_label(&CheckPointLoc, &CheckPointTLI, &backupEndRequired,
                         &backupFromStandby)) {
        // Archive recovery with backup label
        InArchiveRecovery = true;
        if (StandbyModeRequested)
            EnableStandbyMode();

        ereport(LOG, (errmsg("starting backup recovery with redo LSN %X/%X, checkpoint LSN %X/%X, on timeline ID %u",
                            LSN_FORMAT_ARGS(RedoStartLSN),
                            LSN_FORMAT_ARGS(CheckPointLoc),
                            CheckPointTLI)));

        // Read checkpoint record from backup label location
        record = ReadCheckpointRecord(xlogprefetcher, CheckPointLoc, CheckPointTLI);
        if (record != NULL) {
            memcpy(&checkPoint, XLogRecGetData(xlogreader), sizeof(CheckPoint));
            wasShutdown = ((record->xl_info & ~XLR_INFO_MASK) == XLOG_CHECKPOINT_SHUTDOWN);
            InRecovery = true;

            // Verify REDO location exists
            if (checkPoint.redo < CheckPointLoc) {
                XLogPrefetcherBeginRead(xlogprefetcher, checkPoint.redo);
                if (!ReadRecord(xlogprefetcher, LOG, false, checkPoint.ThisTimeLineID))
                    ereport(FATAL, (errmsg("could not find redo location")));
            }
        } else {
            ereport(FATAL, (errmsg("could not locate required checkpoint record")));
        }

        // Process tablespace_map if present
        if (read_tablespace_map(&tablespaces)) {
            // Create tablespace symlinks
            foreach(lc, tablespaces) {
                tablespaceinfo *ti = lfirst(lc);
                char *linkloc = psprintf("pg_tblspc/%u", ti->oid);
                remove_tablespace_symlink(linkloc);
                if (symlink(ti->path, linkloc) < 0)
                    ereport(ERROR, (errmsg("could not create symbolic link")));
            }
            haveTblspcMap = true;
        }
        haveBackupLabel = true;
    } else {
        // No backup_label file - normal startup or crash recovery

        // Clean up orphaned tablespace_map if present
        if (stat(TABLESPACE_MAP, &st) == 0) {
            unlink(TABLESPACE_MAP_OLD);
            durable_rename(TABLESPACE_MAP, TABLESPACE_MAP_OLD, DEBUG1);
        }

        // Determine if archive recovery should be entered
        if (ArchiveRecoveryRequested &&
            (ControlFile->minRecoveryPoint != InvalidXLogRecPtr ||
             ControlFile->backupEndRequired ||
             ControlFile->backupEndPoint != InvalidXLogRecPtr ||
             ControlFile->state == DB_SHUTDOWNED)) {
            InArchiveRecovery = true;
            if (StandbyModeRequested)
                EnableStandbyMode();
        }

        // Get checkpoint from control file
        CheckPointLoc = ControlFile->checkPoint;
        CheckPointTLI = ControlFile->checkPointCopy.ThisTimeLineID;
        RedoStartLSN = ControlFile->checkPointCopy.redo;
        RedoStartTLI = ControlFile->checkPointCopy.ThisTimeLineID;

        record = ReadCheckpointRecord(xlogprefetcher, CheckPointLoc, CheckPointTLI);
        if (record == NULL)
            ereport(PANIC, (errmsg("could not locate a valid checkpoint record")));

        memcpy(&checkPoint, XLogRecGetData(xlogreader), sizeof(CheckPoint));
        wasShutdown = ((record->xl_info & ~XLR_INFO_MASK) == XLOG_CHECKPOINT_SHUTDOWN);
    }

    // Log recovery type and target
    if (ArchiveRecoveryRequested) {
        if (StandbyModeRequested)
            ereport(LOG, (errmsg("entering standby mode")));
        else if (recoveryTarget == RECOVERY_TARGET_XID)
            ereport(LOG, (errmsg("starting point-in-time recovery to XID %u", recoveryTargetXid)));
        // ... other recovery target types
        else
            ereport(LOG, (errmsg("starting archive recovery")));
    }

    // Validate timeline consistency
    if (tliOfPointInHistory(CheckPointLoc, expectedTLEs) != CheckPointTLI) {
        XLogRecPtr switchpoint = tliSwitchPoint(CheckPointTLI, expectedTLEs, NULL);
        ereport(FATAL, (errmsg("requested timeline %u is not a child of this server's history",
                              recoveryTargetTLI)));
    }

    // Validate minimum recovery point timeline
    if (!XLogRecPtrIsInvalid(ControlFile->minRecoveryPoint) &&
        tliOfPointInHistory(ControlFile->minRecoveryPoint - 1, expectedTLEs) !=
        ControlFile->minRecoveryPointTLI)
        ereport(FATAL, (errmsg("requested timeline does not contain minimum recovery point")));

    // Determine if recovery is needed
    if (checkPoint.redo < CheckPointLoc) {
        if (wasShutdown)
            ereport(PANIC, (errmsg("invalid redo record in shutdown checkpoint")));
        InRecovery = true;
    } else if (ControlFile->state != DB_SHUTDOWNED)
        InRecovery = true;
    else if (ArchiveRecoveryRequested)
        InRecovery = true;

    // Update control file state for recovery
    if (InRecovery) {
        if (InArchiveRecovery) {
            ControlFile->state = DB_IN_ARCHIVE_RECOVERY;
        } else {
            ereport(LOG, (errmsg("database system was not properly shut down; automatic recovery in progress")));
            ControlFile->state = DB_IN_CRASH_RECOVERY;
        }

        ControlFile->checkPoint = CheckPointLoc;
        ControlFile->checkPointCopy = checkPoint;

        // Initialize minRecoveryPoint for archive recovery
        if (InArchiveRecovery && ControlFile->minRecoveryPoint < checkPoint.redo) {
            ControlFile->minRecoveryPoint = checkPoint.redo;
            ControlFile->minRecoveryPointTLI = checkPoint.ThisTimeLineID;
        }

        // Set backup points if starting from backup
        if (haveBackupLabel) {
            ControlFile->backupStartPoint = checkPoint.redo;
            ControlFile->backupEndRequired = backupEndRequired;
            if (backupFromStandby)
                ControlFile->backupEndPoint = ControlFile->minRecoveryPoint;
        }
    }

    // Set global recovery state variables
    backupStartPoint = ControlFile->backupStartPoint;
    backupEndRequired = ControlFile->backupEndRequired;
    backupEndPoint = ControlFile->backupEndPoint;

    if (InArchiveRecovery) {
        minRecoveryPoint = ControlFile->minRecoveryPoint;
        minRecoveryPointTLI = ControlFile->minRecoveryPointTLI;
    } else {
        minRecoveryPoint = InvalidXLogRecPtr;
        minRecoveryPointTLI = 0;
    }

    // Initialize recovery state
    abortedRecPtr = InvalidXLogRecPtr;
    missingContrecPtr = InvalidXLogRecPtr;

    // Set output parameters
    *wasShutdown_ptr = wasShutdown;
    *haveBackupLabel_ptr = haveBackupLabel;
    *haveTblspcMap_ptr = haveTblspcMap;
}
```

Key simplifications made:
- Removed detailed error messages and hints for clarity
- Consolidated multiple similar logging statements
- Abstracted complex validation logic into high-level comments
- Removed platform-specific code paths
- Simplified nested conditional structures
- Reduced verbose debugging output statements
- Focused on the main recovery logic flow while preserving essential functionality