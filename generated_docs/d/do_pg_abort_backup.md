# do_pg_abort_backup

## Location
[src/backend/access/transam/xlog.c:9410-9436](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L9410-L9436)

## Overview
Aborts a running backup operation by taking the system out of backup mode, providing a safe cleanup mechanism for error handling and process termination scenarios.

## Definition

```c
void
do_pg_abort_backup(int code, Datum arg)
```
## Detailed Description
This function performs essential cleanup when a backup operation needs to be aborted, either due to errors during backup setup or backend process termination. It safely decrements the running backup counter and resets the session backup state. The function is designed to be called from error handlers and cleanup routines, making it much safer than calling the full do_pg_backup_stop() function in error conditions.

The function handles two scenarios:
1. **During backup start** (arg=true): Called when backup setup fails, where sessionBackupState hasn't been modified yet but runningBackups has been incremented
2. **During process exit** (arg=false): Called as a before_shmem_exit handler when a backend exits with an active backup

## Parameters / Member Variables
- `code`: Exit code (used in callback signature, not functionally used)
- `arg`: Boolean Datum indicating if called during backup start (true) or as exit handler (false)
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

## Simplified Source

```c
// Simplified version of do_pg_abort_backup
void do_pg_abort_backup(int code, Datum arg) {
    bool during_backup_start = DatumGetBool(arg);

    // Validate backup state
    Assert(!during_backup_start || sessionBackupState == SESSION_BACKUP_NONE);

    // Cleanup if backup is active or being started
    if (during_backup_start || sessionBackupState != SESSION_BACKUP_NONE) {
        WALInsertLockAcquireExclusive();
        Assert(XLogCtl->Insert.runningBackups > 0);
        XLogCtl->Insert.runningBackups--;

        sessionBackupState = SESSION_BACKUP_NONE;
        WALInsertLockRelease();

        // Warn if aborting due to unexpected exit
        if (!during_backup_start)
            ereport(WARNING,
                    errmsg("aborting backup due to backend exiting before pg_backup_stop was called"));
    }
}
```

Key simplifications made:
- Removed detailed comments while preserving essential cleanup logic
- Maintained critical assertions for backup state validation
- Preserved the dual-mode operation (startup vs exit handler)
- Kept proper locking for backup counter management
- Maintained warning for unexpected backup termination