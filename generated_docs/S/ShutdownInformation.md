# ShutdownInformation

## Location
src/bin/pg_dump/parallel.c: 146 - 150

## Overview
A structure that holds state information needed for the archive_close_connection() shutdown callback in PostgreSQL's pg_dump parallel processing system.

## Definition


## Detailed Description
ShutdownInformation is a simple structure designed to pass essential state information to the shutdown callback function archive_close_connection(). This structure is used as part of pg_dump's cleanup mechanism to ensure proper shutdown of database connections and worker processes during both normal termination and error conditions. The structure is registered with the on_exit_nicely handler to guarantee clean resource management.

## Parameters / Member Variables
- : Pointer to ParallelState structure containing information about parallel processing state. NULL if not running in parallel mode.
- : Pointer to Archive structure representing the database connection that needs to be closed during shutdown.

## Dependencies
- Functions called/Symbols referenced:
  - [ParallelState](../P/ParallelState.md) (parallel processing state)
  - [Archive](../A/Archive.md) (database connection information)
- Called from (representative examples):
  - [archive_close_connection](../a/archive_close_connection.md) (as callback parameter)
  - shutdown_info (static global variable of this type)

## Notes and Other Information
- Used specifically as a parameter to the archive_close_connection() shutdown callback function
- A static global variable 'shutdown_info' of this type is maintained in parallel.c
- Essential for proper cleanup in both parallel and non-parallel pg_dump operations
- Helps distinguish between leader and worker processes during shutdown to perform appropriate cleanup actions
- Part of PostgreSQL's pg_dump utility shutdown safety mechanism
- Registered with on_exit_nicely to ensure cleanup occurs even during abnormal termination