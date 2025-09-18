# ParallelBackupStart

## Location
src/bin/pg_dump/parallel.c: 897 - 1058

## Overview
Initializes and starts parallel backup/restore operations by spawning worker processes or threads to handle multiple concurrent backup tasks.

## Definition


## Detailed Description
ParallelBackupStart is the main entry point for setting up parallel processing in pg_dump/pg_restore operations. The function creates a specified number of worker processes (on Unix) or threads (on Windows) to enable concurrent backup or restore operations. It establishes communication channels between the leader process and each worker, manages process lifecycle, and sets up proper signal handling. For single-worker scenarios, it returns immediately with minimal setup. The function handles platform-specific differences between Unix fork-based workers and Windows thread-based workers.

## Parameters / Member Variables
- : Archive handle containing configuration including the number of workers to create and database connection information

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc, pg_malloc0 (memory allocation)
  - pgpipe (pipe creation for IPC)
  - fork (Unix process creation)
  - _beginthreadex (Windows thread creation)
  - RunWorker (worker main function)
  - set_archive_cancel_info (query cancellation setup)
  - set_cancel_pstate (signal handling setup)
  - pqsignal (signal handler registration)
  - closesocket (cleanup of unused pipe ends)
- Called from (representative examples):
  - RestoreArchive (src/bin/pg_dump/pg_backup_archiver.c:731)
  - _CloseArchive (src/bin/pg_dump/pg_backup_directory.c:576)

## Notes and Other Information
- Returns a ParallelState structure containing worker management information and communication pipes
- On Unix systems, uses fork() to create worker processes; on Windows, uses threads
- Establishes bidirectional communication pipes between leader and each worker
- Temporarily disables query cancellation during worker creation to prevent inheritance issues
- Sets up proper signal handling to ensure clean shutdown of all workers
- For numWorkers == 1, returns early with minimal parallel state setup
- Workers inherit a copy of the ArchiveHandle but with separate database connections