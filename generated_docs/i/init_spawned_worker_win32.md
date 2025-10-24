# init_spawned_worker_win32

## Location
[src/bin/pg_dump/parallel.c:874-896](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/parallel.c#L874-L896)

## Overview
Windows-specific thread entry point function that initializes and runs a worker thread for parallel backup operations in pg_dump.

## Definition
static unsigned __stdcall init_spawned_worker_win32(WorkerInfo *wi)

## Detailed Description
This function serves as the thread entry point for Windows worker threads in pg_dump's parallel backup system. It acts as a wrapper around the platform-independent RunWorker function, handling Windows-specific thread initialization and cleanup requirements.

The function extracts the ArchiveHandle and ParallelSlot from the WorkerInfo structure, frees the temporary WorkerInfo allocation, executes the main worker logic via RunWorker, and properly terminates the thread using Windows-specific thread exit functions.

## Parameters / Member Variables
- wi: Pointer to WorkerInfo structure containing the ArchiveHandle and ParallelSlot needed for worker initialization

## Dependencies
- Functions called/Symbols referenced:
  - [RunWorker](../R/RunWorker.md)
  - [WorkerInfo](../W/WorkerInfo.md) (type)
  - [ParallelSlot](../P/ParallelSlot.md) (type)
  - [ParallelState](../P/ParallelState.md) (type)
- Called from (representative examples):
  - [ParallelBackupStart](../P/ParallelBackupStart.md)

## Notes and Other Information
- Windows-specific function using __stdcall calling convention
- Static function - only accessible within the parallel.c compilation unit
- Frees the WorkerInfo structure to prevent memory leaks
- Uses _endthreadex(0) for proper Windows thread termination
- Returns 0 to satisfy the function signature requirements, though the thread exits before returning
- Part of the Windows threading implementation that parallels the Unix fork-based approach

## Simplified Source

```c
static unsigned __stdcall
init_spawned_worker_win32(WorkerInfo *wi)
{
    // Extract worker parameters
    ArchiveHandle *AH = wi->AH;
    ParallelSlot *slot = wi->slot;

    // Free temporary WorkerInfo structure
    free(wi);

    // Run the main worker logic
    RunWorker(AH, slot);

    // Terminate thread properly on Windows
    _endthreadex(0);
    return 0;
}
```