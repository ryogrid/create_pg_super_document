# ParallelBackupStart

## Location
[src/bin/pg_dump/parallel.c:897-1058](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/parallel.c#L897-L1058)

## Overview
Initializes and starts parallel backup/restore operations by spawning worker processes or threads to handle multiple concurrent backup tasks.

## Definition

```c
structure to pass args to worker function */
		wi = (WorkerInfo *) pg_malloc(sizeof(WorkerInfo));
```
## Detailed Description
ParallelBackupStart is the main entry point for setting up parallel processing in pg_dump/pg_restore operations. The function creates a specified number of worker processes (on Unix) or threads (on Windows) to enable concurrent backup or restore operations. It establishes communication channels between the leader process and each worker, manages process lifecycle, and sets up proper signal handling. For single-worker scenarios, it returns immediately with minimal setup. The function handles platform-specific differences between Unix fork-based workers and Windows thread-based workers.

## Parameters / Member Variables
- : Archive handle containing configuration including the number of workers to create and database connection information

## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc](../p/pg_malloc.md), pg_malloc0 (memory allocation)
  - [pgpipe](../p/pgpipe.md) (pipe creation for IPC)
  - fork (Unix process creation)
  - _beginthreadex (Windows thread creation)
  - [RunWorker](../R/RunWorker.md) (worker main function)
  - [set_archive_cancel_info](../s/set_archive_cancel_info.md) (query cancellation setup)
  - [set_cancel_pstate](../s/set_cancel_pstate.md) (signal handling setup)
  - [pqsignal](../p/pqsignal.md) (signal handler registration)
  - closesocket (cleanup of unused pipe ends)
- Called from (representative examples):
  - [RestoreArchive](../R/RestoreArchive.md) (src/bin/pg_dump/pg_backup_archiver.c:731)
  - [_CloseArchive](../C/_CloseArchive.md) (src/bin/pg_dump/pg_backup_directory.c:576)

## Notes and Other Information
- Returns a ParallelState structure containing worker management information and communication pipes
- On Unix systems, uses fork() to create worker processes; on Windows, uses threads
- Establishes bidirectional communication pipes between leader and each worker
- Temporarily disables query cancellation during worker creation to prevent inheritance issues
- Sets up proper signal handling to ensure clean shutdown of all workers
- For numWorkers == 1, returns early with minimal parallel state setup
- Workers inherit a copy of the ArchiveHandle but with separate database connections

## Simplified Source

```c
ParallelState *
ParallelBackupStart(ArchiveHandle *AH)
{
    ParallelState *pstate;
    int i;

    // Allocate and initialize parallel state
    pstate = (ParallelState *) pg_malloc(sizeof(ParallelState));
    pstate->numWorkers = AH->public.numWorkers;
    pstate->te = NULL;
    pstate->parallelSlot = NULL;

    // Single worker mode - no parallel processing needed
    if (AH->public.numWorkers == 1)
        return pstate;

    // Allocate arrays for worker status and task entries
    pstate->te = (TocEntry **) pg_malloc0(pstate->numWorkers * sizeof(TocEntry *));
    pstate->parallelSlot = (ParallelSlot *) pg_malloc0(pstate->numWorkers * sizeof(ParallelSlot));

    // Set up shutdown handling and disable cancellation during worker creation
    shutdown_info.pstate = pstate;
    set_archive_cancel_info(AH, NULL);
    fflush(NULL);  // Ensure stdio state is clean before forking

    // Create worker processes/threads
    for (i = 0; i < pstate->numWorkers; i++) {
        ParallelSlot *slot = &(pstate->parallelSlot[i]);
        int pipeMW[2], pipeWM[2];

        // Create communication pipes
        if (pgpipe(pipeMW) < 0 || pgpipe(pipeWM) < 0)
            pg_fatal("could not create communication channels: %m");

        // Set up pipe endpoints
        slot->pipeRead = pipeWM[PIPE_READ];
        slot->pipeWrite = pipeMW[PIPE_WRITE];
        slot->pipeRevRead = pipeMW[PIPE_READ];
        slot->pipeRevWrite = pipeWM[PIPE_WRITE];

#ifdef WIN32
        // Windows: Create worker thread
        WorkerInfo *wi = (WorkerInfo *) pg_malloc(sizeof(WorkerInfo));
        wi->AH = AH;
        wi->slot = slot;

        uintptr_t handle = _beginthreadex(NULL, 0, (void *) &init_spawned_worker_win32,
                                         wi, 0, &(slot->threadId));
        slot->hThread = handle;
        slot->workerStatus = WRKR_IDLE;
#else
        // Unix: Fork worker process
        pid_t pid = fork();
        if (pid == 0) {
            // Worker process setup
            slot->pid = getpid();
            signal_info.am_worker = true;

            // Close unused pipe ends and inherited pipes
            closesocket(pipeWM[PIPE_READ]);
            closesocket(pipeMW[PIPE_WRITE]);
            for (int j = 0; j < i; j++) {
                closesocket(pstate->parallelSlot[j].pipeRead);
                closesocket(pstate->parallelSlot[j].pipeWrite);
            }

            // Run worker and exit
            RunWorker(AH, slot);
            exit(0);
        } else if (pid < 0) {
            pg_fatal("could not create worker process: %m");
        }

        // Leader process setup
        slot->pid = pid;
        slot->workerStatus = WRKR_IDLE;
        closesocket(pipeMW[PIPE_READ]);
        closesocket(pipeWM[PIPE_WRITE]);
#endif
    }

    // Final setup after worker creation
#ifndef WIN32
    pqsignal(SIGPIPE, SIG_IGN);  // Ignore broken pipe signals
#endif
    set_archive_cancel_info(AH, AH->connection);  // Re-enable cancellation
    set_cancel_pstate(pstate);  // Enable signal forwarding to workers

    return pstate;
}
```