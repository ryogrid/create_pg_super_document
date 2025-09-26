# logicalrep_launcher_onexit

## Location
src/backend/replication/logical/launcher.c: 820 - 830

## Overview
A cleanup function registered as an exit handler for the logical replication launcher process that resets the launcher PID in shared memory upon process termination.

## Definition

```c
static void
logicalrep_launcher_onexit(int code, Datum arg)
```
## Detailed Description
This static function serves as an exit handler specifically for the logical replication launcher process. It is designed to be called automatically when the launcher process terminates, regardless of whether the termination is normal or abnormal.

The primary purpose of this function is to maintain consistency in the shared memory state by clearing the launcher process ID. When the launcher process exits, it's crucial that other processes in the system know that the launcher is no longer running. This is accomplished by setting the launcher_pid field in the LogicalRepCtx structure to 0, which indicates that no launcher process is currently active.

This cleanup ensures that future attempts to start a new launcher or check launcher status will have accurate information about the current state of the system.

## Parameters / Member Variables
- : Exit code of the terminating process (standard exit handler parameter, not used in this implementation)
- : Datum argument passed to the exit handler (standard parameter, not used in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - LogicalRepCtx (global shared memory context)
- Called from (representative examples):
  - ApplyLauncherMain (src/backend/replication/logical/launcher.c:1140)

## Notes and Other Information
- This is a static function, only accessible within the launcher.c file
- The function follows the standard PostgreSQL exit handler signature (int code, Datum arg)
- Despite receiving exit code and argument parameters, the function doesn't use them as the cleanup is always the same
- The function is registered using before_shmem_exit() to ensure it's called during process shutdown
- Setting launcher_pid to 0 is the standard way to indicate that no launcher process is running in PostgreSQL's logical replication system
- This cleanup is essential for preventing race conditions when starting new launcher processes