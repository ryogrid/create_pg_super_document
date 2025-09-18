# do_pg_abort_backup

## Location
src/backend/access/transam/xlog.c: 9410 - 9436

## Overview
Aborts a running backup operation by taking the system out of backup mode, providing a safe cleanup mechanism for error handling and process termination scenarios.

## Definition


## Detailed Description
This function performs essential cleanup when a backup operation needs to be aborted, either due to errors during backup setup or backend process termination. It safely decrements the running backup counter and resets the session backup state. The function is designed to be called from error handlers and cleanup routines, making it much safer than calling the full do_pg_backup_stop() function in error conditions.

The function handles two scenarios:
1. **During backup start** (arg=true): Called when backup setup fails, where sessionBackupState hasn't been modified yet but runningBackups has been incremented
2. **During process exit** (arg=false): Called as a before_shmem_exit handler when a backend exits with an active backup

## Parameters / Member Variables
- : Exit code (used in callback signature, not functionally used)
- : Boolean Datum indicating if called during backup start (true) or as exit handler (false)

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetBool](../D/DatumGetBool.md)
  - [WALInsertLockAcquireExclusive](../W/WALInsertLockAcquireExclusive.md)
  - [WALInsertLockRelease](../W/WALInsertLockRelease.md)
  - SESSION_BACKUP_NONE
- Called from:
  - [do_pg_backup_start](do_pg_backup_start.md) (as error cleanup callback)
  - [register_persistent_abort_backup_handler](../r/register_persistent_abort_backup_handler.md) (as exit handler)
  - [perform_base_backup](../p/perform_base_backup.md) (error cleanup)

## Notes and Other Information
- Used as both a PG_ENSURE_ERROR_CLEANUP callback and before_shmem_exit handler
- The odd-looking signature (int code, Datum arg) is required for these callback mechanisms
- Acquires exclusive WAL insertion lock to safely update backup counters
- Issues a WARNING when aborting due to backend exit without proper pg_backup_stop call
- Much safer than do_pg_backup_stop() for error conditions as it only performs essential cleanup
- File location: src/backend/access/transam/xlog.c:9410-9436