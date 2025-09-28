# get_backup_status

## Location
[src/backend/access/transam/xlog.c:9117-9135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L9117-L9135)

## Overview
Returns the current session-level backup state to indicate whether a backup is currently running in the session.

## Definition

```c
SessionBackupState
get_backup_status(void)
```
## Detailed Description
This simple utility function provides access to the current session's backup state by returning the value of the global  variable. The function serves as a clean interface for checking whether the current session has an active backup operation in progress.

The function returns one of the SessionBackupState enumeration values, which typically include states such as:
- SESSION_BACKUP_NONE (no backup running)
- SESSION_BACKUP_RUNNING (backup in progress)
- Other backup states as defined by the SessionBackupState enum

This function is commonly used by backup-related operations to verify the current backup state before proceeding with backup start, stop, or status checking operations.

## Parameters / Member Variables
None - this function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - sessionBackupState (global variable)
- Called from (representative examples):
  - [pg_backup_start](../p/pg_backup_start.md)
  - PG_BACKUP_STOP_V2_COLS
  - [SendBaseBackup](../S/SendBaseBackup.md)

## Notes and Other Information
- This is a simple accessor function that provides session-level backup state information
- The function is thread-safe as it only reads a session-local variable
- Used extensively in backup validation and state checking throughout the backup subsystem
- The returned SessionBackupState value determines valid operations for the current session's backup context

## Simplified Source

```c
// Simplified version of get_backup_status
SessionBackupState get_backup_status(void) {
    // Return the current session's backup state
    return sessionBackupState;
}
```

Key simplifications made:
- Function is already very simple - just returns a global variable
- Added clear comment explaining the purpose
- No additional simplification needed as the function is straightforward
- Preserves the essential session-level backup state access pattern