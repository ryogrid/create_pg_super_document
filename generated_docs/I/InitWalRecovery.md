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