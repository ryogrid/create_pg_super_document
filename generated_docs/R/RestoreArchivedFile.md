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
  - BuildRestoreCommand (constructs the restore command string)
  - [PreRestoreCommand](../P/PreRestoreCommand.md)/PostRestoreCommand (signal handling management)
  - pgstat_report_wait_start/pgstat_report_wait_end (wait event reporting)
  - system (executes the restore command)
  - wait_result_is_signal/wait_result_is_any_signal (signal detection utilities)
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