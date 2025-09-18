# InitWalRecovery

## Location
src/backend/access/transam/xlogrecovery.c: 512 - 1026

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
  - readRecoverySignalFile (reads recovery signal files)
  - validateRecoveryParameters (validates recovery configuration)
  - XLogReaderAllocate (creates WAL reader)
  - XLogPrefetcherAllocate (creates WAL prefetcher)
  - read_backup_label (processes backup label file)
  - ReadCheckpointRecord (reads checkpoint records)
  - EnableStandbyMode (enables standby mode when required)
  - OwnLatch (takes ownership of recovery wakeup latch)
  - tliOfPointInHistory (validates timeline consistency)
  - read_tablespace_map (processes tablespace mappings)
- Called from (representative examples):
  - StartupXLOG (main startup sequence coordinator)

## Notes and Other Information
- Central function in PostgreSQL's recovery initialization process
- Handles both crash recovery and archive recovery scenarios
- Sets up critical global variables like InRecovery, InArchiveRecovery, and StandbyMode
- Creates necessary infrastructure for WAL reading and prefetching
- Validates timeline consistency and recovery parameters
- Manages backup label and tablespace map file processing
- Located in src/backend/access/transam/xlogrecovery.c:512-1026
- Coordinates with StartupXLOG for complete startup sequence