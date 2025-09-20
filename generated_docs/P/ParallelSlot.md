# ParallelSlot

## Location
[src/bin/pg_dump/parallel.c:93-126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/parallel.c#L93-L126)

## Overview
ParallelSlot is a struct that represents the private per-parallel-worker state in PostgreSQL's pg_dump utility, managing the status and communication channels for parallel dump operations.

## Definition

```c
struct ParallelSlot
{
	T_WorkerStatus workerStatus;	/* see enum above */

	/* These fields are valid if workerStatus == WRKR_WORKING: */
	ParallelCompletionPtr callback; /* function to call on completion */
	void	   *callback_data;	/* passthrough data for it */

	ArchiveHandle *AH;			/* Archive data worker is using */

	int			pipeRead;		/* leader's end of the pipes */
	int			pipeWrite;
	int			pipeRevRead;	/* child's end of the pipes */
	int			pipeRevWrite;

	/* Child process/thread identity info: */
#ifdef WIN32
	uintptr_t	hThread;
	unsigned int threadId;
#else
	pid_t		pid;
#endif
};
```
## Detailed Description
ParallelSlot serves as the central data structure for managing parallel worker processes in pg_dump operations. Each slot represents one worker and contains all necessary state information for coordinating between the leader process and worker processes/threads. The structure is designed with platform-specific considerations, using different mechanisms for process identification on Windows (threads) versus Unix-like systems (processes). Much of the structure's content is valid only in the leader process, except for the AH field which should only be accessed by worker processes.

## Parameters / Member Variables
- `workerStatus`: Current status of the worker (from T_WorkerStatus enum)
- `callback`: Function pointer to be called when the worker completes its task
- `*callback_data`: User-defined data passed to the completion callback function
- `*AH`: Archive handle containing the data and state information the worker is processing
- `pipeRead`: File descriptor for the leader process to read from the worker
- `pipeWrite`: File descriptor for the leader process to write to the worker
- `pipeRevRead`: File descriptor for the worker process to read from the leader
- `pipeRevWrite`: File descriptor for the worker process to write to the leader
- `hThread`: (Windows only) Handle to the worker thread
- `threadId`: (Windows only) Identifier for the worker thread
- `pid`: (Unix-like systems only) Process ID of the worker process

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