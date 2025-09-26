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