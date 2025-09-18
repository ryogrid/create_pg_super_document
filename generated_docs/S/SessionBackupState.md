# SessionBackupState

## Location
src/include/access/xlog.h: 288 - 298

## Overview
An enumeration that tracks the session-level status of base backup operations in PostgreSQL, used to coordinate backup state within individual database sessions.

## Definition


## Detailed Description
SessionBackupState manages the backup status at the session level, working in parallel with shared memory status to control base backup operations. This enumeration is crucial for preventing concurrent base backup operations within the same session and ensuring consistent backup state management. SESSION_BACKUP_NONE indicates no backup is currently running in the session, while SESSION_BACKUP_RUNNING indicates an active backup operation. The session-level status is updated simultaneously with shared memory counters to maintain consistency between global and local backup states.

## Parameters / Member Variables
- : No base backup is currently running in this session
- : A base backup operation is active in this session

## Dependencies
- Functions called/Symbols referenced:
  - None (enum type definition)
- Called from (representative examples):
  - [do_pg_backup_start](../d/do_pg_backup_start.md) (src/backend/access/transam/xlog.c:9116)
  - [pg_backup_start](../p/pg_backup_start.md) (src/backend/access/transam/xlogfuncs.c:61)
  - [SendBaseBackup](SendBaseBackup.md) (src/backend/backup/basebackup.c:992)
  - [get_backup_status](../g/get_backup_status.md) function (src/include/access/xlog.h:296)

## Notes and Other Information
- Prevents multiple concurrent base backups within a single session
- Works in coordination with global backup state management in shared memory
- Essential for backup consistency and preventing backup conflicts
- Used by both replication backends and normal database connections performing backups
- State transitions are managed through do_pg_backup_start and do_pg_backup_stop functions
- Critical for proper cleanup if backup operations are aborted or interrupted