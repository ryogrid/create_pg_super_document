# backup_wal_range

## Location
src/backend/backup/basebackup_incremental.c: 52 - 61

## Overview
A structure that holds the details of WAL (Write-Ahead Log) ranges extracted from a backup manifest during incremental backup processing.

## Definition


## Detailed Description
The backup_wal_range structure is used to store information about WAL ranges found in backup manifests during incremental backup operations. Each instance represents a continuous range of WAL records identified by a timeline ID and bounded by start and end LSN (Log Sequence Number) positions. This structure is essential for tracking which WAL segments are covered by a particular backup, allowing the system to determine what additional WAL data is needed for incremental backups.

## Parameters / Member Variables
- : Timeline ID that identifies which timeline this WAL range belongs to
- : Starting Log Sequence Number (LSN) position of the WAL range
- : Ending Log Sequence Number (LSN) position of the WAL range

## Dependencies
- Functions called/Symbols referenced:
  - TimeLineID (data type)
  - XLogRecPtr (data type)
- Called from (representative examples):
  - manifest_process_wal_range (creates and populates instances)
  - PrepareForIncrementalBackup (uses instances for backup planning)

## Notes and Other Information
- This structure is primarily used in the context of incremental backup functionality introduced in PostgreSQL
- The WAL ranges help determine the scope of data that needs to be included in incremental backups
- Memory for backup_wal_range instances is typically allocated using palloc() and managed within the backup context
- The structure is located in src/backend/backup/basebackup_incremental.c:47-52