# do_pg_backup_start

## Location
src/backend/access/transam/xlog.c: 8808 - 9116

## Overview
Initiates an online backup by creating a checkpoint, establishing backup state, and constructing a tablespace map for consistent database backup operations.

## Definition


## Detailed Description
This function serves as the core implementation of PostgreSQL's online backup functionality. It orchestrates the complex process of starting a consistent backup while the database remains online and operational. The function handles multiple critical aspects of backup initialization:

- Validates WAL level requirements for online backups
- Increments the running backup counter to force full-page writes
- Forces a checkpoint to ensure data consistency
- Establishes unique backup starting points to avoid conflicts
- Scans and maps all tablespaces for inclusion in the backup
- Constructs a tablespace map file for backup restoration

The function includes special handling for backups started during recovery mode, ensuring proper validation of full-page write requirements and checkpoint positioning. It employs error cleanup mechanisms to ensure proper state management even if the backup initialization fails.

## Parameters / Member Variables
- : Identifier string for the backup (must not exceed MAXPGPATH length)
- : Boolean flag indicating whether to perform an immediate checkpoint for faster backup start
- : Optional output parameter containing list of tablespace information structures
- : Output parameter containing backup state information including start point, timeline, and checkpoint location
- : Output parameter containing tablespace mapping information for backup restoration

## Dependencies
- Functions called/Symbols referenced:
  - RecoveryInProgress
  - XLogIsNeeded
  - WALInsertLockAcquireExclusive
  - WALInsertLockRelease
  - PG_ENSURE_ERROR_CLEANUP
  - do_pg_abort_backup
  - RequestXLogSwitch
  - RequestCheckpoint
  - AllocateDir
  - ReadDir
  - get_dirent_type
  - readlink
  - FreeDir
  - BackupState
  - tablespaceinfo
  - CHECKPOINT_FORCE
  - CHECKPOINT_WAIT
  - CHECKPOINT_IMMEDIATE
  - SESSION_BACKUP_RUNNING
- Called from (representative examples):
  - pg_backup_start
  - perform_base_backup

## Notes and Other Information
- The function enforces full-page writes during backup to prevent torn pages in concurrent read scenarios
- Uses exclusive WAL insertion locks when modifying the running backup counter to ensure proper synchronization
- Implements a retry mechanism to ensure unique backup starting points when multiple backups are initiated simultaneously
- During recovery, skips WAL file switching and uses the last restartpoint as the backup starting checkpoint
- Validates full-page write requirements for backups taken on standby servers to ensure backup integrity
- Constructs comprehensive tablespace mapping including both symbolic links and direct directories
- Proper error cleanup ensures backup counter is decremented if initialization fails
- The tablespace map file is essential for Windows tar-based backups due to symbolic link limitations