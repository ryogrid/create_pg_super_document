# DumpSignalInformation

## Location
src/bin/pg_dump/parallel.c: 163 - 171

## Overview
DumpSignalInformation is a struct that maintains state information for signal handling in pg_dump's parallel processing operations, managing database connection cancellation and worker process coordination.

## Definition


## Detailed Description
DumpSignalInformation serves as the central data structure for managing signal handling in pg_dump's parallel operations. The structure maintains critical state needed for proper cleanup and cancellation when signals (like SIGINT) are received. The design accounts for platform differences between Unix-like systems and Windows, with different semantics for connection management. On Unix systems, each process has its own connection that myAH points to, while on Windows, there's a single instance where myAH points to the leader connection and worker connections are accessed through the parallel state structure. The structure is used as a static volatile global variable (signal_info) to ensure signal handlers can access the necessary state for cleanup operations.

## Parameters / Member Variables
- : Pointer to the ArchiveHandle containing the database connection that should be cancelled when a signal is received
- : Pointer to the ParallelState structure containing information about all parallel workers and their state
- : Boolean flag indicating whether signal handlers have been installed in the current process
- : (Unix-like systems only) Boolean flag indicating whether the current process is a worker process rather than the leader

## Dependencies
- Functions called/Symbols referenced:
  - ArchiveHandle
  - ParallelState
- Called from (representative examples):
  - sigTermHandler (signal handler function)
  - consoleHandler (Windows console handler)
  - set_cancel_handler
  - set_cancel_pstate
  - ParallelBackupStart

## Notes and Other Information
- Used as a static volatile global variable 'signal_info' that is initialized to zero
- Protected by a critical section (signal_info_lock) on Windows to ensure thread safety
- The structure enables proper cleanup of database connections and worker processes when interrupted
- Platform-specific behavior: on Unix each process maintains its own connection, on Windows the leader connection is shared and worker connections are accessed indirectly
- Essential for graceful shutdown and cancellation of long-running pg_dump operations
- The volatile qualifier ensures that signal handlers can safely access the global instance even with compiler optimizations