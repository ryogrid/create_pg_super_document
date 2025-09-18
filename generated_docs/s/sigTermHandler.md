# sigTermHandler

## Location
src/bin/pg_dump/parallel.c: 545 - 607

## Overview
Unix-only signal handler function that manages graceful termination of pg_dump processes in both single-process and parallel operation modes, forwarding termination signals to worker processes and canceling active database operations.

## Definition
```c
static void sigTermHandler(SIGNAL_ARGS)
```

## Detailed Description
sigTermHandler is a signal handler function designed to handle termination signals (SIGINT, SIGTERM, SIGQUIT) in pg_dump operations. The function implements a multi-step shutdown process to ensure clean termination:

1. **Signal masking**: Immediately disables further signal delivery to prevent interruption during cleanup
2. **Worker forwarding**: If running as a leader process in parallel mode, forwards the termination signal to all worker processes
3. **Query cancellation**: Cancels any active database queries using PostgreSQL's query cancellation mechanism
4. **Error reporting**: Reports termination status to stderr (leader process only)
5. **Process termination**: Exits using _exit() to avoid potential issues with atexit handlers

The function is designed to work safely in signal handler context, using only async-signal-safe functions and avoiding complex operations that could deadlock or corrupt process state.

## Parameters / Member Variables
- `SIGNAL_ARGS`: Standard signal handler arguments (signal number and additional platform-specific information)

## Dependencies
- Functions called/Symbols referenced:
  - [pqsignal](../p/pqsignal.md) (installs/modifies signal handlers)
  - kill (sends SIGTERM to worker processes)
  - [PQcancel](../P/PQcancel.md) (cancels active PostgreSQL queries)
  - [write_stderr](../w/write_stderr.md) (writes error messages to stderr)
  - _exit (terminates process without cleanup)
- Global variables accessed:
  - signal_info.pstate (parallel state information)
  - signal_info.myAH (archive handle for query cancellation)
  - signal_info.am_worker (flag indicating worker vs leader process)
  - progname (program name for error reporting)
- Called from (representative examples):
  - Signal delivery mechanism (not directly called by application code)

## Notes and Other Information
- This is a static function and Unix-only (not compiled on Windows platforms)
- Function operates in signal handler context with strict limitations on callable functions
- Uses _exit() instead of exit() to avoid potential deadlocks with atexit handlers
- Implements signal masking to prevent recursive signal delivery during cleanup
- Worker processes are terminated before query cancellation to minimize invalid-snapshot errors
- Only leader processes report termination messages to avoid duplicate output
- Function accesses global signal_info structure that must be properly initialized before signal installation
- Located in src/bin/pg_dump/parallel.c:545-607