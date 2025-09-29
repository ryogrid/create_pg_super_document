# read_backup_label

## Location
[src/backend/access/transam/xlogrecovery.c:1208-1353](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L1208-L1353)

## Overview
Reads and parses the backup_label file during recovery to determine the correct checkpoint location and timeline for restoring from a backup, ensuring database consistency over pg_control settings.

## Definition

```c
struct a valid data directory.")));
```
## Detailed Description
This function checks for the presence of a backup_label file and parses its contents during WAL recovery initialization. When a backup_label file exists, it indicates recovery from a backup dump, and the function extracts critical recovery parameters from the file rather than relying on pg_control. This prevents consistency issues that could arise if pg_control was archived after the backup started.

The function parses various fields from the backup_label file:
- START WAL LOCATION: Starting LSN and timeline for recovery
- CHECKPOINT LOCATION: Checkpoint location to start from  
- BACKUP METHOD: Determines if streamed backup requiring end-of-backup processing
- BACKUP FROM: Indicates if backup was taken from primary or standby
- START TIME/LABEL: Optional fields for debugging
- START TIMELINE: Sanity check field (PostgreSQL 11+)
- INCREMENTAL FROM LSN: Detects incremental backups (not supported for direct recovery)

## Parameters / Member Variables
- : Output parameter for checkpoint location from backup_label
- : Output parameter for timeline ID from backup_label  
- : Output parameter indicating if this is a streamed backup requiring end-of-backup processing
- : Output parameter indicating if backup was taken from a standby server

## Dependencies
- Functions called/Symbols referenced:
  - [AllocateFile](../A/AllocateFile.md) (opens backup_label file for reading)
  - [FreeFile](../F/FreeFile.md) (closes the backup_label file)
  - BACKUP_LABEL_FILE (backup_label filename constant)
  - MAXFNAMELEN (maximum filename length constant)
- Called from:
  - [InitWalRecovery](../I/InitWalRecovery.md) (during WAL recovery initialization)

## Notes and Other Information
- Returns false if backup_label file doesn't exist (normal case)
- Returns true if backup_label found and parsed successfully
- Sets global variables RedoStartLSN and RedoStartTLI from the backup file
- File parsing is intentionally crude but sufficient for the fixed format
- Issues FATAL error for malformed backup_label files
- Detects and rejects incremental backups which require pg_combinebackup tool
- Timeline consistency is verified between WAL segment and timeline fields
- Handles optional fields gracefully for backward compatibility

## Simplified Source

```c
// Simplified version of read_backup_label
static bool
read_backup_label(XLogRecPtr *checkPointLoc, TimeLineID *backupLabelTLI,
                  bool *backupEndRequired, bool *backupFromStandby)
{
    FILE *lfp;
    char backuptype[20];
    char backupfrom[20];
    uint32 hi, lo;
    TimeLineID tli_from_walseg, tli_from_file;

    // Initialize output parameters
    *checkPointLoc = InvalidXLogRecPtr;
    *backupLabelTLI = 0;
    *backupEndRequired = false;
    *backupFromStandby = false;

    // Try to open backup_label file
    lfp = AllocateFile(BACKUP_LABEL_FILE, "r");
    if (!lfp) {
        if (errno != ENOENT)
            ereport(FATAL, (errmsg("could not read backup_label file")));
        return false;  // No backup_label file found - normal case
    }

    // Parse START WAL LOCATION line
    if (fscanf(lfp, "START WAL LOCATION: %X/%X (file %*s)%*c", &hi, &lo, &tli_from_walseg) < 3)
        ereport(FATAL, (errmsg("invalid backup_label format")));
    RedoStartLSN = ((uint64) hi) << 32 | lo;
    RedoStartTLI = tli_from_walseg;

    // Parse CHECKPOINT LOCATION line
    if (fscanf(lfp, "CHECKPOINT LOCATION: %X/%X%*c", &hi, &lo) < 2)
        ereport(FATAL, (errmsg("invalid backup_label format")));
    *checkPointLoc = ((uint64) hi) << 32 | lo;
    *backupLabelTLI = tli_from_walseg;

    // Check backup method - determines if we need to wait for backup end
    if (fscanf(lfp, "BACKUP METHOD: %19s", backuptype) == 1) {
        if (strcmp(backuptype, "streamed") == 0)
            *backupEndRequired = true;
    }

    // Check backup source - primary vs standby
    if (fscanf(lfp, "BACKUP FROM: %19s", backupfrom) == 1) {
        if (strcmp(backupfrom, "standby") == 0)
            *backupFromStandby = true;
    }

    // Parse optional fields (START TIME, LABEL for debugging)
    fscanf(lfp, "START TIME: %*[^\n]");
    fscanf(lfp, "LABEL: %*[^\n]");

    // Verify timeline consistency if START TIMELINE present
    if (fscanf(lfp, "START TIMELINE: %u", &tli_from_file) == 1) {
        if (tli_from_walseg != tli_from_file)
            ereport(FATAL, (errmsg("timeline mismatch in backup_label")));
    }

    // Reject incremental backups
    if (fscanf(lfp, "INCREMENTAL FROM LSN: %X/%X", &hi, &lo) > 0)
        ereport(FATAL, (errmsg("incremental backup not supported for direct recovery")));

    // Clean up and return success
    if (ferror(lfp) || FreeFile(lfp))
        ereport(FATAL, (errmsg("error reading backup_label file")));

    return true;
}
```

Key simplifications made:
- Removed detailed variable declarations and combined related ones
- Simplified error messages to be more concise while preserving meaning
- Used `%*s` and `%*c` format specifiers to skip unneeded parsed values
- Consolidated file parsing error handling
- Removed detailed debugging reports (kept essential error cases)
- Simplified format string parsing by focusing on core required fields
- Abstracted complex error code details into simpler messages
- Maintained all essential logic flow and functionality