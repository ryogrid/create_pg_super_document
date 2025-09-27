# RestoreArchivedFile

## Location
[src/fe_utils/archive.c:39-108](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/archive.c#L39-L108)

## Overview
RestoreArchivedFile attempts to retrieve a specified WAL (Write-Ahead Logging) file from off-line archival storage using the configured restore_command, providing a critical mechanism for PostgreSQL's point-in-time recovery and archive recovery functionality.

## Definition

```c
struct stat stat_buf;
```
## Detailed Description
RestoreArchivedFile is a core function in PostgreSQL's WAL archive recovery system that attempts to restore WAL files from external archive storage. The function executes the user-configured restore_command to copy archived WAL files back to the database's WAL directory during recovery operations. It implements robust error handling and validation to ensure the integrity of restored files, including size verification and proper cleanup of temporary files.

The function prioritizes archived files over local copies to ensure robustness during recovery, as local files might be incomplete or corrupted. It also supports cleanup management by calculating archive file cutoff points based on restart points, allowing old WAL files to be safely deleted from the archive after they're no longer needed for recovery.

When the restore operation fails (which is expected when reaching the end of available WAL), the function falls back to attempting to use any local WAL file that might exist in the XLOGDIR.

## Parameters / Member Variables
- : Output parameter that will contain the full path to the restored file (temporary name) on success, or the normal on-line file path on failure
- : The name of the WAL file to restore from the archive
- : The temporary filename to use for the restored file in the WAL directory
- : Expected size of the file for validation (set to 0 if size is unknown)
- : When false, prevents deletion of old WAL segments in the archive (used during initial checkpoint record fetching)

## Dependencies
- Functions called/Symbols referenced:
  - [GetOldestRestartPoint](../G/GetOldestRestartPoint.md) (retrieves restart point information for cleanup calculations)
  - XLByteToSeg (converts WAL position to segment number)
  - [XLogFileName](../X/XLogFileName.md) (generates WAL filename from timeline and segment)
  - [BuildRestoreCommand](../B/BuildRestoreCommand.md) (constructs the restore command string)
  - [PreRestoreCommand](../P/PreRestoreCommand.md)/PostRestoreCommand (signal handling management)
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md)/pgstat_report_wait_end (wait event reporting)
  - system (executes the restore command)
  - [wait_result_is_signal](../w/wait_result_is_signal.md)/wait_result_is_any_signal (signal detection utilities)
  - [proc_exit](../p/proc_exit.md) (process termination)
- Called from (representative examples):
  - [XLogFileRead](../X/XLogFileRead.md) (in xlogrecovery.c:4210)
  - [restoreTimeLineHistoryFiles](../r/restoreTimeLineHistoryFiles.md) (in timeline.c:62)
  - [readTimeLineHistory](../r/readTimeLineHistory.md) (in timeline.c:100)
  - [SimpleXLogPageRead](../S/SimpleXLogPageRead.md) (in pg_rewind/parsexlog.c:341)

## Notes and Other Information
- The function only operates during archive recovery (not crash recovery), checking ArchiveRecoveryRequested flag
- Implements sophisticated signal handling during restore command execution, treating SIGTERM, SIGINT, and SIGQUIT as reasons to abort recovery
- Uses temporary filenames to avoid conflicts and ensure atomic operations
- Supports both standby mode and regular recovery scenarios with different error handling behaviors
- Size mismatches are treated as DEBUG1 in standby mode (for partial files being copied) but FATAL in regular recovery
- The restore command's exit status is carefully analyzed to distinguish between missing archives and actual command failures
- Falls back to local XLOGDIR files when archive restoration fails, enabling recovery from locally available WAL segments
- Maintains detailed logging at various levels (DEBUG1, DEBUG2, DEBUG3, LOG, FATAL) for troubleshooting recovery issues

## Simplified Source

```c
// Simplified version of RestoreArchivedFile
bool RestoreArchivedFile(char *path, const char *xlogfname,
                        const char *recovername, off_t expectedSize,
                        bool cleanupEnabled) {
    char xlogpath[MAXPGPATH];
    char *xlogRestoreCmd;
    char lastRestartPointFname[MAXPGPATH];
    int rc;
    struct stat stat_buf;

    // Skip if not in archive recovery mode
    if (!ArchiveRecoveryRequested) {
        goto not_available;
    }

    // Skip if no restore command configured
    if (recoveryRestoreCommand == NULL || strcmp(recoveryRestoreCommand, "") == 0) {
        goto not_available;
    }

    // Build temporary file path in WAL directory
    snprintf(xlogpath, MAXPGPATH, XLOGDIR "/%s", recovername);

    // Remove any existing temporary file
    if (stat(xlogpath, &stat_buf) == 0) {
        if (unlink(xlogpath) != 0) {
            ereport(FATAL, (errmsg("could not remove file \"%s\": %m", xlogpath)));
        }
    }

    // Calculate cleanup cutoff point for archive management
    if (cleanupEnabled) {
        XLogRecPtr restartRedoPtr;
        TimeLineID restartTli;
        XLogSegNo restartSegNo;

        GetOldestRestartPoint(&restartRedoPtr, &restartTli);
        XLByteToSeg(restartRedoPtr, restartSegNo, wal_segment_size);
        XLogFileName(lastRestartPointFname, restartTli, restartSegNo, wal_segment_size);
    } else {
        XLogFileName(lastRestartPointFname, 0, 0, wal_segment_size);
    }

    // Build and execute restore command
    xlogRestoreCmd = BuildRestoreCommand(recoveryRestoreCommand, xlogpath,
                                        xlogfname, lastRestartPointFname);

    ereport(DEBUG3, (errmsg_internal("executing restore command \"%s\"", xlogRestoreCmd)));

    // Execute restore command with proper signal handling
    PreRestoreCommand();
    rc = system(xlogRestoreCmd);
    PostRestoreCommand();

    pfree(xlogRestoreCmd);

    // Check if restore succeeded
    if (rc == 0) {
        // Verify restored file exists and has correct size
        if (stat(xlogpath, &stat_buf) == 0) {
            if (expectedSize > 0 && stat_buf.st_size != expectedSize) {
                // Handle size mismatch (different behavior for standby vs normal mode)
                int elevel = (StandbyMode && stat_buf.st_size < expectedSize) ? DEBUG1 : FATAL;
                ereport(elevel, (errmsg("archive file \"%s\" has wrong size", xlogfname)));
                return false;
            } else {
                // Success: return path to restored file
                ereport(LOG, (errmsg("restored log file \"%s\" from archive", xlogfname)));
                strcpy(path, xlogpath);
                return true;
            }
        } else {
            // Restore command succeeded but file missing
            ereport(LOG, (errmsg("could not stat file \"%s\"", xlogpath)));
        }
    }

    // Handle restore command failure
    if (wait_result_is_signal(rc, SIGTERM)) {
        proc_exit(1);
    }

    ereport(wait_result_is_any_signal(rc, true) ? FATAL : DEBUG2,
            (errmsg("could not restore file \"%s\" from archive", xlogfname)));

not_available:
    // Fallback: try local file in XLOGDIR
    snprintf(path, MAXPGPATH, XLOGDIR "/%s", xlogfname);
    return false;
}
```

Key simplifications made:
- Removed extensive comments and consolidated into clear code flow
- Abstracted complex error handling while preserving essential safety checks
- Simplified signal handling logic but kept SIGTERM handling
- Maintained core archive recovery logic: check prerequisites, build command, execute, verify
- Preserved size validation and standby vs normal mode differences
- Kept the essential fallback mechanism to local files
- Reduced from ~247 lines to ~65 lines while preserving critical functionality