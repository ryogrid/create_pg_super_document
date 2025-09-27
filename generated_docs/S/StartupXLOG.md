# StartupXLOG

## Location
[src/backend/access/transam/xlog.c:5384-6187](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L5384-L6187)

## Overview
StartupXLOG is the main recovery function that must be called ONCE during postmaster or standalone-backend startup to perform WAL (Write-Ahead Log) recovery and bring the database system to a consistent state.

## Definition

```c
struct a RunningTransactions snapshot representing a
				 * shut down server, with only prepared transactions still
				 * alive. We're never overflowed at this point because all
				 * subxids are listed with their parent prepared transactions.
				 */
				running.xcnt = nxids;
```
## Detailed Description
StartupXLOG is a comprehensive function responsible for orchestrating the entire database recovery process during startup. It handles multiple recovery scenarios including clean shutdowns, crash recovery, and archive recovery (point-in-time recovery). The function performs the following major operations:

1. **Control File Validation**: Examines the control file state to determine the database's previous shutdown condition and validates checkpoint locations
2. **Directory Structure Setup**: Ensures WAL directory structure exists and removes temporary files from previous crashes
3. **Recovery Initialization**: Sets up recovery state, initializes shared memory structures, and prepares for WAL replay
4. **WAL Recovery**: Performs actual WAL record replay if needed, restoring the database to a consistent state
5. **Timeline Management**: Handles timeline switching for archive recovery scenarios
6. **System Transition**: Transitions the database from recovery mode to production mode

The function handles various database states (DB_SHUTDOWNED, DB_IN_CRASH_RECOVERY, DB_IN_ARCHIVE_RECOVERY, etc.) and performs appropriate recovery actions for each scenario. It also manages Hot Standby initialization, prepared transaction recovery, and ensures proper synchronization of shared memory structures.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [ValidateXLOGDirectoryStructure](../V/ValidateXLOGDirectoryStructure.md)
  - [InitWalRecovery](../I/InitWalRecovery.md)
  - [PerformWalRecovery](../P/PerformWalRecovery.md)
  - [FinishWalRecovery](../F/FinishWalRecovery.md)
  - [PrescanPreparedTransactions](../P/PrescanPreparedTransactions.md)
  - [RecoverPreparedTransactions](../R/RecoverPreparedTransactions.md)
  - [findNewestTimeLine](../f/findNewestTimeLine.md)
  - [writeTimeLineHistory](../w/writeTimeLineHistory.md)
  - [XLogInitNewTimeline](../X/XLogInitNewTimeline.md)
  - [StartupCLOG](StartupCLOG.md), StartupMultiXact, StartupReplicationSlots
  - [RemoveTempXlogFiles](../R/RemoveTempXlogFiles.md)
  - [ResetUnloggedRelations](../R/ResetUnloggedRelations.md)
  - [PerformRecoveryXLogAction](../P/PerformRecoveryXLogAction.md)
  - [PreallocXlogFiles](../P/PreallocXlogFiles.md)
- Called from (representative examples):
  - [StartupProcessMain](StartupProcessMain.md) (startup process entry point)
  - [InitPostgres](../I/InitPostgres.md) (single-user mode startup)

## Notes and Other Information
- Must be called exactly once during database startup
- Sets up resource owner context for auxiliary processes
- Handles both crash recovery (extending current timeline) and archive recovery (creating new timeline)  
- Manages transition from InRecovery=true to InRecovery=false state
- Critical for database consistency - ensures all committed transactions are replayed
- Coordinates with Hot Standby functionality when enabled
- Handles cleanup of backup label and tablespace map files after successful recovery
- Updates control file state to DB_IN_PRODUCTION upon completion
- Located in src/backend/access/transam/xlog.c:5384-6187

## Simplified Source

```c
// Simplified version of StartupXLOG
void StartupXLOG(void) {
    CheckPoint checkPoint;
    bool wasShutdown, didCrash, haveBackupLabel, haveTblspcMap;
    XLogRecPtr EndOfLog;
    TimeLineID EndOfLogTLI, newTLI;
    bool performedWalRecovery;
    EndOfWalRecoveryInfo *endOfRecoveryInfo;

    // Set up resource owner for aux process
    Assert(AuxProcessResourceOwner != NULL);
    CurrentResourceOwner = AuxProcessResourceOwner;

    // Validate control file checkpoint location
    if (!XRecOffIsValid(ControlFile->checkPoint)) {
        ereport(FATAL, "control file contains invalid checkpoint location");
    }

    // Check database state and report accordingly
    switch (ControlFile->state) {
        case DB_SHUTDOWNED:
            ereport(LOG, "database system was shut down normally");
            break;
        case DB_IN_CRASH_RECOVERY:
            ereport(LOG, "database system was interrupted during recovery");
            break;
        // ... other states handled similarly
        default:
            ereport(FATAL, "control file contains invalid database state");
    }

    // Validate WAL directory structure
    ValidateXLOGDirectoryStructure();

    // Set up startup progress timeout handler
    if (!IsBootstrapProcessingMode()) {
        RegisterTimeout(STARTUP_PROGRESS_TIMEOUT, startup_progress_timeout_handler);
    }

    // Clean up after crash if needed
    if (ControlFile->state != DB_SHUTDOWNED &&
        ControlFile->state != DB_SHUTDOWNED_IN_RECOVERY) {
        RemoveTempXlogFiles();
        SyncDataDirectory();
        didCrash = true;
    } else {
        didCrash = false;
    }

    // Initialize WAL recovery
    InitWalRecovery(ControlFile, &wasShutdown, &haveBackupLabel, &haveTblspcMap);
    checkPoint = ControlFile->checkPointCopy;

    // Initialize shared memory variables from checkpoint
    TransamVariables->nextXid = checkPoint.nextXid;
    TransamVariables->nextOid = checkPoint.nextOid;
    // ... other transam variables initialized

    // Remove old relcache files
    RelationCacheInitFileRemove();

    // Initialize various subsystems
    StartupReplicationSlots();
    StartupReorderBuffer();
    StartupCLOG();
    StartupMultiXact();
    if (ControlFile->track_commit_timestamp) {
        StartupCommitTs();
    }
    StartupReplicationOrigin();

    // Set up unlogged LSN
    if (ControlFile->state == DB_SHUTDOWNED) {
        pg_atomic_write_membarrier_u64(&XLogCtl->unloggedLSN, ControlFile->unloggedLSN);
    } else {
        pg_atomic_write_membarrier_u64(&XLogCtl->unloggedLSN, FirstNormalUnloggedLSN);
    }

    // Restore timeline history files and 2PC data
    restoreTimeLineHistoryFiles(checkPoint.ThisTimeLineID, recoveryTargetTLI);
    restoreTwoPhaseData();

    // Reset or restore pgstat data
    if (didCrash) {
        pgstat_discard_stats();
    } else {
        pgstat_restore_stats();
    }

    // Set up initial WAL replay state
    RedoRecPtr = XLogCtl->RedoRecPtr = checkPoint.redo;
    lastFullPageWrites = checkPoint.fullPageWrites;

    // Perform WAL recovery if needed
    if (InRecovery) {
        // Set recovery state in shared memory
        SpinLockAcquire(&XLogCtl->info_lck);
        XLogCtl->SharedRecoveryState = InArchiveRecovery ?
            RECOVERY_STATE_ARCHIVE : RECOVERY_STATE_CRASH;
        SpinLockRelease(&XLogCtl->info_lck);

        // Update control file
        UpdateControlFile();

        // Clean up backup label and tablespace map files
        if (haveBackupLabel) {
            durable_rename(BACKUP_LABEL_FILE, BACKUP_LABEL_OLD, FATAL);
        }
        if (haveTblspcMap) {
            durable_rename(TABLESPACE_MAP, TABLESPACE_MAP_OLD, FATAL);
        }

        // Set up Hot Standby if enabled
        if (ArchiveRecoveryRequested && EnableHotStandby) {
            InitRecoveryTransactionEnvironment();
            // ... Hot Standby initialization
        }

        // Perform actual WAL recovery
        PerformWalRecovery();
        performedWalRecovery = true;
    } else {
        performedWalRecovery = false;
    }

    // Finish WAL recovery
    endOfRecoveryInfo = FinishWalRecovery();
    EndOfLog = endOfRecoveryInfo->endOfLog;
    EndOfLogTLI = endOfRecoveryInfo->endOfLogTLI;

    // Validate recovery completed properly
    if (InRecovery && EndOfLog < LocalMinRecoveryPoint) {
        ereport(FATAL, "WAL ends before consistent recovery point");
    }

    // Reset unlogged relations
    if (InRecovery) {
        ResetUnloggedRelations(UNLOGGED_RELATION_INIT);
    }

    // Determine new timeline ID
    newTLI = endOfRecoveryInfo->lastRecTLI;
    if (ArchiveRecoveryRequested) {
        newTLI = findNewestTimeLine(recoveryTargetTLI) + 1;
        XLogInitNewTimeline(EndOfLogTLI, EndOfLog, newTLI);
        writeTimeLineHistory(newTLI, recoveryTargetTLI, EndOfLog,
                            endOfRecoveryInfo->recoveryStopReason);
    }

    // Save timeline ID in shared memory
    SpinLockAcquire(&XLogCtl->info_lck);
    XLogCtl->InsertTimeLineID = newTLI;
    SpinLockRelease(&XLogCtl->info_lck);

    // Prepare for WAL writing
    SetInstallXLogFileSegmentActive();
    // ... initialize WAL buffers and positions

    // Initialize various systems for production
    SIResetAll();
    PreallocXlogFiles(EndOfLog, newTLI);

    // Mark system as no longer in recovery
    InRecovery = false;

    // Start archive timeout timer
    XLogCtl->lastSegSwitchTime = time(NULL);
    XLogCtl->lastSegSwitchLSN = EndOfLog;

    // Complete subsystem initialization
    TrimCLOG();
    TrimMultiXact();
    RecoverPreparedTransactions();
    ShutdownWalRecovery();

    // Enable WAL writes and emit checkpoint/end-of-recovery record
    LocalSetXLogInsertAllowed();
    if (performedWalRecovery) {
        PerformRecoveryXLogAction();
    }

    // Update control file to production state
    LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
    ControlFile->state = DB_IN_PRODUCTION;
    SpinLockAcquire(&XLogCtl->info_lck);
    XLogCtl->SharedRecoveryState = RECOVERY_STATE_DONE;
    SpinLockRelease(&XLogCtl->info_lck);
    UpdateControlFile();
    LWLockRelease(ControlFileLock);

    // Final cleanup
    if (standbyState != STANDBY_DISABLED) {
        ShutdownRecoveryTransactionEnvironment();
    }
    WalSndWakeup(true, true);
}
```

Key simplifications made:
- Condensed the very detailed error state handling into simpler cases
- Abstracted complex shared memory initialization into high-level comments
- Simplified Hot Standby setup logic while preserving the essential flow
- Removed extensive comments and consolidated similar operations
- Focused on the main recovery workflow: validate → recover → transition to production
- Maintained all critical state transitions and synchronization points
- Reduced from ~800 lines to ~150 lines while preserving essential logic