# on_proc_exit

## Location
src/backend/storage/ipc/ipc.c: 309 - 336

## Overview
The on_proc_exit function registers cleanup callback functions to be executed during PostgreSQL process termination, providing a mechanism for components to ensure proper resource cleanup.

## Definition


## Detailed Description
on_proc_exit implements the callback registration system for PostgreSQL process termination cleanup. It adds function pointers and their associated arguments to the on_proc_exit_list array, which will be executed in reverse order during process termination by proc_exit_prepare(). The function includes safety checks to prevent buffer overflow and automatically registers the atexit_callback as a system atexit handler on first use. This design ensures that cleanup callbacks are executed both during normal termination (via proc_exit) and emergency termination (via direct exit() calls), providing comprehensive coverage for resource cleanup scenarios.

## Parameters / Member Variables
- : Callback function pointer of type pg_on_exit_callback to be executed during termination
- : Datum argument to be passed to the callback function when executed

## Dependencies
- Functions called/Symbols referenced:
  - atexit_callback (registered as system atexit handler)
  - ereport (for error reporting when limit exceeded)
  - MAX_ON_EXITS (maximum number of registerable callbacks)
- Called from (representative examples):
  - PostgresMain (main backend initialization)
  - PostmasterMain (postmaster process setup)
  - InitCatCache (catalog cache initialization)
  - llvm_session_initialize (LLVM JIT cleanup)
  - smgrinit (storage manager initialization)

## Notes and Other Information
- Limited to MAX_ON_EXITS registered callbacks to prevent resource exhaustion
- Callbacks are executed in reverse registration order (LIFO) during termination
- First registration automatically sets up atexit_callback as system atexit handler
- atexit_callback_setup flag prevents multiple atexit registrations
- Throws FATAL error if registration limit is exceeded
- Used throughout PostgreSQL for cleanup of various subsystems and resources
- Callbacks receive the exit code and their registered argument when executed
- Part of PostgreSQL's comprehensive resource management and cleanup system