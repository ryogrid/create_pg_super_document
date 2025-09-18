# ParallelSlot

## Location
[src/bin/pg_dump/parallel.c:93-126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/parallel.c#L93-L126)

## Overview
ParallelSlot is a struct that represents the private per-parallel-worker state in PostgreSQL's pg_dump utility, managing the status and communication channels for parallel dump operations.

## Definition


## Detailed Description
ParallelSlot serves as the central data structure for managing parallel worker processes in pg_dump operations. Each slot represents one worker and contains all necessary state information for coordinating between the leader process and worker processes/threads. The structure is designed with platform-specific considerations, using different mechanisms for process identification on Windows (threads) versus Unix-like systems (processes). Much of the structure's content is valid only in the leader process, except for the AH field which should only be accessed by worker processes.

## Parameters / Member Variables
- : Current status of the worker (from T_WorkerStatus enum)
- : Function pointer to be called when the worker completes its task
- : User-defined data passed to the completion callback function
- : Archive handle containing the data and state information the worker is processing
- : File descriptor for the leader process to read from the worker
- : File descriptor for the leader process to write to the worker
- : File descriptor for the worker process to read from the leader
- : File descriptor for the worker process to write to the leader
- : (Windows only) Handle to the worker thread
- : (Windows only) Identifier for the worker thread
- : (Unix-like systems only) Process ID of the worker process

## Dependencies
- Functions called/Symbols referenced:
  - [T_WorkerStatus](../T/T_WorkerStatus.md)
  - ParallelCompletionPtr
  - [ArchiveHandle](../A/ArchiveHandle.md)
  - pid_t
- Called from (representative examples):
  - [init_parallel_dump_utils](../i/init_parallel_dump_utils.md)
  - [ParallelBackupStart](ParallelBackupStart.md)
  - [ListenToWorkers](../L/ListenToWorkers.md)
  - [WaitForTerminatingWorkers](../W/WaitForTerminatingWorkers.md)
  - [ParallelSlotsSetup](ParallelSlotsSetup.md)

## Notes and Other Information
- The structure is designed for cross-platform compatibility with separate implementations for Windows (thread-based) and Unix-like systems (process-based)
- Access patterns are carefully controlled: leader process manages most fields, while workers should only touch the AH field
- The pipe descriptors enable bidirectional communication between leader and worker processes
- Used extensively in pg_dump's parallel processing functionality and also referenced in other PostgreSQL utilities like pg_amcheck, reindexdb, and vacuumdb
- Part of PostgreSQL's frontend utilities parallel processing framework