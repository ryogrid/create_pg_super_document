# register_persistent_abort_backup_handler

## Location
src/backend/access/transam/xlog.c: 9437 - 9450

## Overview
Registers a session exit handler that will warn about unterminated backups when a backend process exits, ensuring backup cleanup even if pg_backup_stop is not properly called.

## Definition


## Detailed Description
This function sets up a persistent cleanup mechanism for backup operations by registering do_pg_abort_backup as a before_shmem_exit handler. The handler ensures that if a backend process terminates while a backup is running (without proper cleanup via pg_backup_stop), the backup state will be properly cleaned up and a warning will be issued.

The function uses a static boolean flag to ensure the handler is only registered once per backend process, preventing duplicate handler registrations that could lead to multiple cleanup attempts.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - before_shmem_exit
  - do_pg_abort_backup
  - DatumGetBool
- Called from:
  - pg_backup_start (src/backend/access/transam/xlogfuncs.c:96)

## Notes and Other Information
- Uses a static boolean flag (already_done) to prevent multiple registrations
- The handler is registered with DatumGetBool(false) to indicate it's being called as an exit handler (not during backup start)
- Provides safety net for backup cleanup when backends terminate unexpectedly
- Essential for maintaining backup counter consistency and preventing resource leaks
- Part of the backup infrastructure's defensive programming approach
- File location: src/backend/access/transam/xlog.c:9437-9450