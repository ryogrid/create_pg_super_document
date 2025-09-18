# perform_base_backup

## Location
src/backend/backup/basebackup.c: 234 - 683

## Overview
 is the core function that executes the actual base backup process for specified tablespaces, handling the complete workflow from backup initialization to cleanup.

## Definition


## Detailed Description
This function orchestrates the entire base backup process in PostgreSQL. It begins by calling  to initiate the backup, then systematically processes each tablespace, creating tar archives for the data. The function handles both regular and incremental backups, and optionally includes WAL files in the backup. It uses error cleanup mechanisms to ensure proper cleanup even if the backup fails partway through.

The function operates in several key phases:
1. Initialize backup state and call 
2. Calculate total backup size if progress reporting is enabled
3. Send backup_label and tablespace_map files
4. Process each tablespace, creating tar archives
5. Handle WAL file inclusion if requested
6. Finalize backup with manifest and cleanup

Key safety features include comprehensive error handling with PG_ENSURE_ERROR_CLEANUP to prevent backup counter leaks, validation of WAL file sequences, and checksum verification.

## Parameters / Member Variables
- : Configuration options controlling backup behavior (progress reporting, WAL inclusion, etc.)
- : Output destination handler for writing backup data 
- : Incremental backup information, NULL for full backups

## Dependencies
- Functions called/Symbols referenced:
  - do_pg_backup_start
  - do_pg_backup_stop
  - sendDir
  - sendTablespace
  - sendFileWithContent
  - build_backup_content
  - CheckXLogRemoved
  - compareWalFileNames
  - bbsink_begin_backup/bbsink_end_backup
  - RecoveryInProgress
- Called from (representative examples):
  - SendBaseBackup

## Notes and Other Information
- This function is static and split out primarily to avoid compiler warnings about variables potentially being clobbered by longjmp
- Uses extensive error cleanup with PG_ENSURE_ERROR_CLEANUP to ensure backup counters are properly managed
- Performs comprehensive validation of WAL file sequences when including WAL
- The main data directory is always processed last to facilitate WAL inclusion
- Supports both full and incremental backups through the IncrementalBackupInfo parameter