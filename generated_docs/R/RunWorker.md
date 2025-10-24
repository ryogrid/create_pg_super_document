# RunWorker

## Location
[src/bin/pg_dump/parallel.c:829-873](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/parallel.c#L829-L873)

## Overview
Sets up and executes the main worker process/thread loop for parallel backup operations in pg_dump, managing the worker's lifecycle from initialization to cleanup.

## Definition
static void RunWorker(ArchiveHandle *AH, ParallelSlot *slot)

## Detailed Description
This function serves as the entry point for worker processes or threads in pg_dump's parallel backup system. It performs complete worker lifecycle management, including cloning the archive handle to create an isolated state, setting up cancellation support, executing the worker-specific setup function, processing commands from the main process via pipes, and performing cleanup when done.

The function works on both Unix (fork-based) and Windows (thread-based) platforms, using the same code path but with different underlying process/thread creation mechanisms. It ensures that each worker has its own database connection and archive state while maintaining proper cancellation capabilities.

## Parameters / Member Variables
- AH: The source ArchiveHandle to be cloned for this worker's use
- slot: ParallelSlot structure containing pipe file descriptors and worker state information

## Dependencies
- Functions called/Symbols referenced:
  - [CloneArchive](../C/CloneArchive.md)
  - [set_cancel_slot_archive](../s/set_cancel_slot_archive.md)
  - [WaitForCommands](../W/WaitForCommands.md)
  - [DisconnectDatabase](../D/DisconnectDatabase.md)
  - [DeCloneArchive](../D/DeCloneArchive.md)
  - [ParallelSlot](../P/ParallelSlot.md) (type)
  - PIPE_READ, PIPE_WRITE (constants)
- Called from (representative examples):
  - [init_spawned_worker_win32](../i/init_spawned_worker_win32.md)
  - [ParallelBackupStart](../P/ParallelBackupStart.md)

## Notes and Other Information
- Static function - only accessible within the parallel.c compilation unit
- Caller should exit the process or thread upon return from this function
- Clones the archive even on Unix systems for state isolation and connection management
- Registers and unregisters the archive handle with the slot for proper signal handling
- Calls the archive-specific SetupWorkerPtr function to perform format-specific initialization
- Handles both process-based (Unix fork) and thread-based (Windows) parallel execution models

## Simplified Source

```c
static void
RunWorker(ArchiveHandle *AH, ParallelSlot *slot)
{
    // Set up pipe file descriptors for communication
    int pipefd[2];
    pipefd[PIPE_READ] = slot->pipeRevRead;
    pipefd[PIPE_WRITE] = slot->pipeRevWrite;

    // Clone archive for isolated worker state and DB connection
    AH = CloneArchive(AH);

    // Register archive with slot for signal handling
    set_cancel_slot_archive(slot, AH);

    // Call archive-specific worker setup function
    (AH->SetupWorkerPtr)((Archive *) AH);

    // Execute commands from main process until done
    WaitForCommands(AH, pipefd);

    // Cleanup: unregister archive, disconnect DB, destroy clone
    set_cancel_slot_archive(slot, NULL);
    DisconnectDatabase(&(AH->public));
    DeCloneArchive(AH);
}
```