# DumpSignalInformation

## Location
[src/bin/pg_dump/parallel.c:163-171](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/parallel.c#L163-L171)

## Overview
DumpSignalInformation is a struct that maintains state information for signal handling in pg_dump's parallel processing operations, managing database connection cancellation and worker process coordination.

## Definition

```c
typedef struct DumpSignalInformation
{
	ArchiveHandle *myAH;		/* database connection to issue cancel for */
	ParallelState *pstate;		/* parallel state, if any */
	bool		handler_set;	/* signal handler set up in this process? */
#ifndef WIN32
	bool		am_worker;		/* am I a worker process? */
#endif
} DumpSignalInformation;
```
## Detailed Description
DumpSignalInformation serves as the central data structure for managing signal handling in pg_dump's parallel operations. The structure maintains critical state needed for proper cleanup and cancellation when signals (like SIGINT) are received. The design accounts for platform differences between Unix-like systems and Windows, with different semantics for connection management. On Unix systems, each process has its own connection that myAH points to, while on Windows, there's a single instance where myAH points to the leader connection and worker connections are accessed through the parallel state structure. The structure is used as a static volatile global variable (signal_info) to ensure signal handlers can access the necessary state for cleanup operations.

## Parameters / Member Variables
- `*myAH`: Pointer to the ArchiveHandle containing the database connection that should be cancelled when a signal is received
- `*pstate`: Pointer to the ParallelState structure containing information about all parallel workers and their state
- `handler_set`: Boolean flag indicating whether signal handlers have been installed in the current process
- `am_worker`: (Unix-like systems only) Boolean flag indicating whether the current process is a worker process rather than the leader
## Dependencies
- Functions called/Symbols referenced:
  - [ArchiveHandle](../A/ArchiveHandle.md)
  - [ParallelState](../P/ParallelState.md)
- Called from (representative examples):
  - [sigTermHandler](../s/sigTermHandler.md) (signal handler function)
  - [consoleHandler](../c/consoleHandler.md) (Windows console handler)
  - [set_cancel_handler](../s/set_cancel_handler.md)
  - [set_cancel_pstate](../s/set_cancel_pstate.md)
  - [ParallelBackupStart](../P/ParallelBackupStart.md)

## Notes and Other Information
- Used as a static volatile global variable 'signal_info' that is initialized to zero
- Protected by a critical section (signal_info_lock) on Windows to ensure thread safety
- The structure enables proper cleanup of database connections and worker processes when interrupted
- Platform-specific behavior: on Unix each process maintains its own connection, on Windows the leader connection is shared and worker connections are accessed indirectly
- Essential for graceful shutdown and cancellation of long-running pg_dump operations
- The volatile qualifier ensures that signal handlers can safely access the global instance even with compiler optimizations