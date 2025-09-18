# pg_backup_start

## Location
[src/backend/access/transam/xlogfuncs.c:56-122](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogfuncs.c#L56-L122)

## Overview
Initiates an online backup by creating the necessary setup for backup_label file and tablespace map generation.

## Definition


## Detailed Description
The  function sets up the infrastructure needed for taking an online backup dump of a PostgreSQL database. It validates that no backup is currently in progress, allocates necessary memory contexts for backup state management, and delegates the actual backup initialization to . The function ensures proper memory management by creating a dedicated backup context that persists until  is called.

The function performs several key operations:
1. Validates that no backup is already running in the current session
2. Creates or resets a dedicated memory context for backup operations
3. Allocates backup state and tablespace map structures
4. Registers an abort handler for cleanup on errors
5. Calls the core backup initialization function
6. Returns the starting LSN for the backup

## Parameters / Member Variables
-  (text): User-supplied label string that identifies this backup (typically indicates where the backup will be stored)
-  (bool): Whether to use fast backup mode (affects checkpoint behavior)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP
  - PG_GETARG_BOOL
  - [get_backup_status](../g/get_backup_status.md)
  - text_to_cstring
  - AllocSetContextCreate
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [palloc0](palloc0.md)
  - makeStringInfo
  - [register_persistent_abort_backup_handler](../r/register_persistent_abort_backup_handler.md)
  - [do_pg_backup_start](../d/do_pg_backup_start.md)
  - PG_RETURN_LSN
- Called from (representative examples):
  - No direct callers found (SQL function interface)

## Notes and Other Information
- This is a PostgreSQL SQL function accessible through the GRANT system for permission management
- Memory allocated in the backup context persists until pg_backup_stop() is called
- If an error occurs before backup completion, memory may leak until the next pg_backup_start() call
- Only one backup can run per session - attempting multiple concurrent backups will raise an error
- The function returns the starting LSN (Log Sequence Number) of the backup
- Part of PostgreSQL's online backup infrastructure located in src/backend/access/transam/xlogfuncs.c:56-100