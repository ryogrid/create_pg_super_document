# pgarch_die

## Location
[src/backend/postmaster/pgarch.c:845-858](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/pgarch.c#L845-L858)

## Overview
A static cleanup handler function that performs essential cleanup when the PostgreSQL archiver process exits, ensuring proper process state management.

## Definition
```c
static void pgarch_die(int code, Datum arg)
```

## Detailed Description
`pgarch_die` is an exit-time cleanup handler specifically designed for the PostgreSQL archiver process. The functions primary responsibility is to clean up the archivers process state when the process terminates, whether through normal shutdown or abnormal exit conditions.

The function performs a critical cleanup operation by invalidating the archivers process number in the shared `PgArch` structure. This ensures that other parts of the PostgreSQL system know that the archiver process is no longer active and prevents any attempts to communicate with or reference a defunct process.

This cleanup is essential for maintaining the integrity of PostgreSQLs process management system and ensuring that process slots can be properly reused when the archiver is restarted.

## Parameters / Member Variables
- `code`: Exit code indicating the reason for termination (not used in current implementation)
- `arg`: Additional argument data passed to the cleanup handler (not used in current implementation)

## Dependencies
- Functions called/Symbols referenced:
  - `INVALID_PROC_NUMBER`: Constant used to mark an invalid/unassigned process number
  - `PgArch->pgprocno`: Global archiver state field that tracks the process number

- Called from (representative examples):
  - [PgArchiverMain](../P/PgArchiverMain.md): Registered as an exit cleanup handler during archiver initialization
  - Process exit handling infrastructure when the archiver process terminates

## Notes and Other Information
- This is a static function, only accessible within the pgarch.c source file
- The function signature matches PostgreSQLs exit callback interface requirements
- Currently, the function parameters are not used, but they provide extensibility for future cleanup needs
- The cleanup is minimal but critical - it ensures proper process state management
- This function is typically registered as an `on_shmem_exit` callback to ensure it runs during process termination
- Proper cleanup prevents resource leaks and ensures the archiver slot can be reused by future archiver processes
- The function is designed to be safe to call multiple times or in various exit scenarios