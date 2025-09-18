# proc_exit

## Location
src/backend/storage/ipc/ipc.c: 104 - 164

## Overview
The proc_exit function is the main process termination function in PostgreSQL that ensures proper cleanup of resources before exiting. It is designed to be the only function that should call exit() directly in the PostgreSQL system.

## Definition


## Detailed Description
proc_exit serves as the central exit point for PostgreSQL processes, implementing a two-phase shutdown strategy. It first calls proc_exit_prepare() to handle all cleanup operations, then performs optional profiling setup (when PROFILE_PID_DIR is defined), and finally calls the system exit() function. The function includes safety checks to prevent execution in child processes and handles special profiling directory creation for debugging builds. This design ensures that all registered cleanup callbacks are executed before process termination, maintaining system consistency and preventing resource leaks.

## Parameters / Member Variables
- : Exit status code to be passed to the system exit() function

## Dependencies
- Functions called/Symbols referenced:
  - [proc_exit_prepare](proc_exit_prepare.md)
  - AmAutoVacuumWorkerProcess (conditionally)
  - mkdir (conditionally) 
  - elog
- Called from (representative examples):
  - [PostgresMain](../P/PostgresMain.md) (main backend process)
  - [AutoVacWorkerMain](../A/AutoVacWorkerMain.md) (autovacuum worker)
  - [WalReceiverMain](../W/WalReceiverMain.md) (WAL receiver process)
  - ProcessInterrupts (interrupt handling)
  - [errfinish](../e/errfinish.md) (error handling)

## Notes and Other Information
- Should be the only function calling exit() directly in PostgreSQL
- Includes safety check using MyProcPid to prevent execution in child processes
- Conditionally creates profiling directories when PROFILE_PID_DIR is defined
- Autovacuum workers get special profiling directory treatment to prevent disk bloat
- An atexit callback is also registered as backup for cases where exit() is called directly
- Used across all PostgreSQL process types including backends, background workers, and utility processes