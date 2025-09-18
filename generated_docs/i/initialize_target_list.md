# initialize_target_list

## Location
[src/backend/backup/basebackup_target.c:172-193](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_target.c#L172-L193)

## Overview
Initializes the global BaseBackupTargetTypeList with built-in backup target types, setting up the foundation for the backup target system.

## Definition
```c
static void initialize_target_list(void)
```

## Detailed Description
This static function populates the BaseBackupTargetTypeList with predefined backup target types from the builtin_backup_targets array. It switches to TopMemoryContext to ensure the list entries persist across memory context changes, then iterates through the array of built-in targets, appending each one to the global list until it encounters a NULL-terminated entry.

The function is called lazily by other functions (BaseBackupAddTarget and BaseBackupGetTargetHandle) when they need to ensure the target list is initialized. This approach allows the system to start with a minimal set of core backup targets while supporting dynamic extension through BaseBackupAddTarget.

## Parameters / Member Variables
- No parameters (static function with void parameter list)

## Dependencies
- Functions called/Symbols referenced:
  - [BaseBackupTargetType](../B/BaseBackupTargetType.md) (struct)
  - builtin_backup_targets (static array)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - TopMemoryContext
  - lappend
  - BaseBackupTargetTypeList (global variable)

- Called from (representative examples):
  - [BaseBackupAddTarget](../B/BaseBackupAddTarget.md)
  - [BaseBackupGetTargetHandle](../B/BaseBackupGetTargetHandle.md)

## Notes and Other Information
- Static function, not exported outside this compilation unit
- Lazily initializes the backup target system on first use
- Built-in targets include "blackhole" and "server" target types
- The "blackhole" target discards backup data (useful for testing)
- The "server" target writes backups to server-side directories
- Uses TopMemoryContext to ensure target list persists across memory contexts
- Called automatically when needed, no manual initialization required
- Part of PostgreSQL's extensible backup target architecture