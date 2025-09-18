# readRecoverySignalFile

## Location
[src/backend/access/transam/xlogrecovery.c:1027-1108](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L1027-L1108)

## Overview
Detects and processes PostgreSQL recovery signal files to determine the appropriate recovery mode and sets up recovery state variables accordingly.

## Definition
```c
static void readRecoverySignalFile(void)
```

## Detailed Description
readRecoverySignalFile is a static function that scans for PostgreSQL recovery signal files and configures the server's recovery behavior based on their presence. The function checks for the deprecated recovery.conf file (and fails if found), removes any leftover recovery.done files, and then looks for the current recovery signal files: standby.signal and recovery.signal. The standby signal file takes precedence and enables both standby mode and archive recovery, while the recovery signal file enables only archive recovery. The function also performs file synchronization on detected signal files to ensure persistence and validates that standby mode is not requested in single-user server environments.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - IsBootstrapProcessingMode (checks if in bootstrap mode)
  - [stat](../s/stat.md) (checks file existence)
  - unlink (removes recovery.done file)
  - BasicOpenFilePerm (opens signal files)
  - pg_fsync (synchronizes signal files to disk)
  - close (closes file descriptors)
  - RECOVERY_COMMAND_FILE, STANDBY_SIGNAL_FILE, RECOVERY_SIGNAL_FILE (file path constants)
- Called from (representative examples):
  - [InitWalRecovery](../I/InitWalRecovery.md) (during WAL recovery initialization at line 540)

## Notes and Other Information
- Static function, only accessible within xlogrecovery.c
- Part of PostgreSQL's recovery system introduced in version 12+
- Replaces the old recovery.conf configuration approach
- Sets global variables: StandbyModeRequested, ArchiveRecoveryRequested
- Enforces that standby mode requires postmaster (multi-process environment)
- Handles signal file precedence: standby.signal overrides recovery.signal
- Performs fsync on signal files to ensure durability
- Located in src/backend/access/transam/xlogrecovery.c:1027-1108