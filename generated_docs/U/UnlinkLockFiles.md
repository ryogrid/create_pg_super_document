# UnlinkLockFiles

## Location
[src/backend/utils/init/miscinit.c:1170-1204](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L1170-L1204)

## Overview
A proc_exit callback function that removes PostgreSQL lockfiles during server shutdown and logs the completion of database system shutdown.

## Definition
```c
static void UnlinkLockFiles(int status, Datum arg)
```

## Detailed Description
This function serves as a process exit callback that is automatically registered when lockfiles are created. It iterates through the global `lock_files` list and removes (unlinks) all lockfiles that were created by the current process, including the data directory lockfile ($DATADIR/postmaster.pid) and Unix socket lockfiles ($SOCKFILE.lock). After removing all lockfiles, it logs an appropriate shutdown message - using LOG level for postmaster processes and NOTICE level for standalone backends to reduce verbosity. This ensures clean shutdown and prevents stale lockfiles from remaining after process termination.

## Parameters / Member Variables
- `status`: Process exit status (standard proc_exit callback parameter)
- `arg`: Additional data argument (standard proc_exit callback parameter, unused here)

## Dependencies
- Functions called/Symbols referenced:
  - `unlink` - System call to remove files from filesystem
  - `NOTICE` - PostgreSQL logging level constant
  - `LOG` - PostgreSQL logging level constant  
  - `ereport` - PostgreSQL error/message reporting function
  - [errmsg](../e/errmsg.md) - PostgreSQL message formatting function
  - `IsPostmasterEnvironment` - Global variable indicating if running in postmaster context
  - `lock_files` - Global list containing paths of all created lockfiles
  - `foreach`, `lfirst` - PostgreSQL list iteration macros
  - `NIL` - PostgreSQL empty list constant
- Called from (representative examples):
  - [CreateLockFile](../C/CreateLockFile.md) - Registered as proc_exit callback when lockfiles are created

## Notes and Other Information
- Function is declared static, making it internal to the miscinit.c compilation unit
- Does not report errors if unlink() fails, following PostgreSQL's philosophy of best-effort cleanup during shutdown
- Memory cleanup is skipped since process is exiting anyway (performance optimization)
- Lockfile removal is intentionally the last externally visible action during shutdown
- The shutdown message helps administrators confirm clean database shutdown
- Only postmaster and standalone backend processes use this callback - child processes exit without calling it
- Part of PostgreSQL's interlock-file support system for preventing multiple server instances