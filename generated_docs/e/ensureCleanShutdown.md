# ensureCleanShutdown

## Location
[src/bin/pg_rewind/pg_rewind.c:1129-1199](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/pg_rewind.c#L1129-L1199)

## Overview
Ensures the target PostgreSQL cluster has undergone a clean shutdown by running postgres in single-user mode to complete crash recovery.

## Definition


## Detailed Description
This function is a critical part of the pg_rewind process that ensures the target cluster is in a consistent state before performing the rewind operation. It works by:

1. Locating the postgres executable in the same directory as the current program
2. Constructing a command to run postgres in single-user mode (--single)
3. Running the command against the template1 database with input redirected from /dev/null
4. The single-user mode execution will perform crash recovery if needed

The function uses specific flags to optimize the recovery process: -F (disable fsync) makes recovery faster since the data directory will be synced at the end of the rewind anyway.

## Parameters / Member Variables
- : The program name/path used to locate the postgres executable in the same directory

## Dependencies
- Functions called/Symbols referenced:
  - find_other_exec
  - find_my_exec
  - strlcpy
  - [pg_fatal](../p/pg_fatal.md)
  - pg_log_info
  - createPQExpBuffer
  - [appendShellString](../a/appendShellString.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - fflush
  - system
  - pg_log_error
  - pg_log_error_detail
  - destroyPQExpBuffer
- Called from (representative examples):
  - [main](../m/main.md) (pg_rewind.c)

## Notes and Other Information
- This is a static function local to pg_rewind.c
- Respects the dry_run global flag - will only locate postgres but skip execution if dry_run is true
- Uses single-user mode with --single flag for safe crash recovery
- Disables fsync (-F flag) for faster recovery since full sync happens later
- Connects to template1 database for recovery execution
- Will terminate the program (exit(1)) if the postgres execution fails
- Supports custom configuration files via the global config_file variable
- Located at src/bin/pg_rewind/pg_rewind.c:1129-1199