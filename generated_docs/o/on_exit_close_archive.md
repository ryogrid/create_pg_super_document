# on_exit_close_archive

## Location
src/bin/pg_dump/parallel.c: 328 - 338

## Overview
Registers a cleanup handler to ensure proper archive connection cleanup when pg_dump or pg_restore processes exit.

## Definition


## Detailed Description
This function serves as a registration mechanism for cleanup operations in pg_dump and pg_restore utilities. It sets up an exit handler that will automatically close archive connections when the process terminates, either normally or abnormally.

The function performs two main operations:
1. **State Storage**: Stores the Archive pointer in the global shutdown_info structure for later use by the cleanup handler
2. **Handler Registration**: Registers the archive_close_connection function as an exit handler using on_exit_nicely()

This design ensures that database connections and archive resources are properly cleaned up even if the process terminates unexpectedly (e.g., due to signals, errors, or user interruption). The cleanup is particularly important in parallel dump operations where multiple connections may be active.

The function is typically called early in the process lifecycle, immediately after the ArchiveHandle is created, to ensure cleanup coverage for the entire operation duration.

## Parameters / Member Variables
- : Pointer to Archive structure representing the archive handle that needs cleanup on process exit

## Dependencies
- Functions called/Symbols referenced:
  - [archive_close_connection](../a/archive_close_connection.md) (cleanup function to be called on exit)
  - [on_exit_nicely](on_exit_nicely.md) (PostgreSQL utility function for registering exit handlers)

- Called from (representative examples):
  - [main](../m/main.md) (in src/bin/pg_dump/pg_dump.c:838)
  - [main](../m/main.md) (in src/bin/pg_dump/pg_restore.c:434)
  - [_tocEntry](../t/_tocEntry.md) (referenced in src/bin/pg_dump/pg_backup_archiver.h:387)

## Notes and Other Information
- This function is called by both pg_dump and pg_restore utilities
- Uses a global shutdown_info structure to maintain state for the exit handler
- The registered cleanup function (archive_close_connection) will be called automatically on process termination
- Essential for preventing resource leaks and ensuring proper connection cleanup
- Part of the robust error handling and cleanup framework in PostgreSQL dump utilities
- Works in conjunction with the parallel dump infrastructure to handle cleanup in multi-threaded scenarios