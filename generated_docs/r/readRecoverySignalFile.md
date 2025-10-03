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



## Dependencies
- Functions called/Symbols referenced:
  - IsBootstrapProcessingMode (checks if in bootstrap mode)
  - [stat](../s/stat.md) (checks file existence)
  - unlink (removes recovery.done file)
  - [BasicOpenFilePerm](../B/BasicOpenFilePerm.md) (opens signal files)
  - [pg_fsync](../p/pg_fsync.md) (synchronizes signal files to disk)
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

## Simplified Source

```c
// Simplified version of readRecoverySignalFile
static void readRecoverySignalFile(void) {
    struct stat stat_buf;

    // Skip processing during bootstrap mode
    if (IsBootstrapProcessingMode())
        return;

    // Check for deprecated recovery.conf and fail if found
    if (stat(RECOVERY_COMMAND_FILE, &stat_buf) == 0)
        ereport(FATAL, (errmsg("recovery.conf is no longer supported")));

    // Clean up any leftover recovery.done file
    unlink(RECOVERY_COMMAND_DONE);

    // Check for standby signal file (takes precedence)
    if (stat(STANDBY_SIGNAL_FILE, &stat_buf) == 0) {
        // Fsync the signal file for durability
        int fd = BasicOpenFilePerm(STANDBY_SIGNAL_FILE, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
        if (fd >= 0) {
            pg_fsync(fd);
            close(fd);
        }
        standby_signal_file_found = true;
    }
    // Otherwise check for recovery signal file
    else if (stat(RECOVERY_SIGNAL_FILE, &stat_buf) == 0) {
        // Fsync the signal file for durability
        int fd = BasicOpenFilePerm(RECOVERY_SIGNAL_FILE, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
        if (fd >= 0) {
            pg_fsync(fd);
            close(fd);
        }
        recovery_signal_file_found = true;
    }

    // Set recovery mode based on which signal file was found
    if (standby_signal_file_found) {
        StandbyModeRequested = true;
        ArchiveRecoveryRequested = true;
    } else if (recovery_signal_file_found) {
        StandbyModeRequested = false;
        ArchiveRecoveryRequested = true;
    } else {
        return; // No recovery needed
    }

    // Prevent standby mode in single-user servers
    if (StandbyModeRequested && !IsUnderPostmaster)
        ereport(FATAL, (errmsg("standby mode requires multi-process server")));
}
```

Key simplifications made:
- Consolidated duplicate file handling logic for signal files
- Simplified error messages for clarity
- Removed detailed error codes and file access specifics
- Combined similar conditional branches
- Added descriptive comments explaining the logic flow
- Streamlined the recovery mode setting logic