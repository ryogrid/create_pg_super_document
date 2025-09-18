# read_backup_label

## Location
[src/backend/access/transam/xlogrecovery.c:1208-1353](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L1208-L1353)

## Overview
Reads and parses the backup_label file during recovery to determine the correct checkpoint location and timeline for restoring from a backup, ensuring database consistency over pg_control settings.

## Definition


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
  - AllocateFile (opens backup_label file for reading)
  - FreeFile (closes the backup_label file)
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