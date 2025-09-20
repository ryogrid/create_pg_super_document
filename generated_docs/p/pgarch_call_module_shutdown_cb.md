# pgarch_call_module_shutdown_cb

## Location
[src/backend/postmaster/pgarch.c:953-957](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/pgarch.c#L953-L957)

## Overview
A callback function that invokes the shutdown callback of the loaded archive module when the PostgreSQL process is terminating.

## Definition

```c
static void
pgarch_call_module_shutdown_cb(int code, Datum arg)
```
## Detailed Description
pgarch_call_module_shutdown_cb is a shutdown callback function that is registered with PostgreSQL's process exit handling mechanism via . Its primary purpose is to ensure that any loaded archive module gets a chance to perform cleanup operations before the archiver process terminates.

The function follows the standard PostgreSQL shutdown callback signature, accepting an exit code and a Datum argument (which is unused in this implementation). It checks if the currently loaded archive module has defined a shutdown callback, and if so, invokes it with the archive module's state.

## Parameters / Member Variables
- : The exit code of the terminating process (not used in this function)
- : A Datum argument passed to the callback (not used in this function)

## Dependencies
- Functions called/Symbols referenced:
  - Uses global variables: ArchiveCallbacks, archive_module_state
- Called from (representative examples):
  - [LoadArchiveLibrary](../L/LoadArchiveLibrary.md) (registers this as a shutdown callback)
  - [arch_files_state](../a/arch_files_state.md) (indirectly through process exit mechanism)

## Notes and Other Information
- This function is registered as a shutdown callback by  using 
- Only calls the shutdown callback if the archive module has defined one ()
- Provides a graceful way for archive modules to perform cleanup before process termination
- Part of PostgreSQL's archive module infrastructure
- The function signature conforms to PostgreSQL's standard shutdown callback interface
- Located in src/backend/postmaster/pgarch.c:953-957