# ParallelState

## Location
[src/bin/pg_dump/parallel.h:55-61](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/parallel.h#L55-L61)

## Overview
ParallelState is a struct that manages the overall state for parallel backup and restore operations in pg_dump, coordinating multiple worker processes/threads to perform database dump and restore tasks concurrently.

## Definition

```c
typedef struct ParallelState
{
	int			numWorkers;		/* allowed number of workers */
	/* these arrays have numWorkers entries, one per worker: */
	TocEntry  **te;				/* item being worked on, or NULL */
	ParallelSlot *parallelSlot; /* private info about each worker */
} ParallelState;
```
## Detailed Description
ParallelState serves as the central coordination structure for pg_dump's parallel processing capabilities. It maintains state information for all worker processes or threads that are executing backup or restore operations concurrently. The struct is designed to track what each worker is currently processing and provides access to the communication infrastructure needed to coordinate work distribution among the workers.

The structure supports both Unix fork()-based processes and Windows thread-based workers, providing a unified interface for parallel operations across different operating systems. It is primarily used by the leader process/thread to manage and coordinate worker activities during parallel dump and restore operations.

## Parameters / Member Variables
- `numWorkers`: The total number of worker processes/threads allowed to run concurrently, as specified by the user
- `**te`: Array of pointers to TocEntry structures, one per worker, indicating which table of contents item each worker is currently processing (NULL if worker is idle)
- `*parallelSlot`: Array of ParallelSlot structures containing private communication and state information for each worker process/thread
## Dependencies
- Functions called/Symbols referenced:
  - [TocEntry](../T/TocEntry.md)
  - [ParallelSlot](ParallelSlot.md)

- Called from (representative examples):
  - [ParallelBackupStart](ParallelBackupStart.md)
  - [ParallelBackupEnd](ParallelBackupEnd.md)
  - [DispatchJobForTocEntry](../D/DispatchJobForTocEntry.md)
  - [GetIdleWorker](../G/GetIdleWorker.md)
  - [ShutdownWorkersHard](../S/ShutdownWorkersHard.md)
  - [ListenToWorkers](../L/ListenToWorkers.md)
  - [WaitForWorkers](../W/WaitForWorkers.md)
  - [RestoreArchive](../R/RestoreArchive.md)

## Notes and Other Information
- The struct is allocated and initialized by ParallelBackupStart() when parallel operations begin
- For single-worker operations (numWorkers == 1), the te and parallelSlot arrays remain NULL as no coordination is needed
- The structure is used in both dump and restore operations, with the same coordination mechanisms applying to both scenarios
- Memory management for the arrays is handled by the parallel subsystem, with proper cleanup performed during shutdown
- The parallelSlot array contains platform-specific information (process IDs on Unix, thread handles on Windows) for communication with workers
- This struct is defined in src/bin/pg_dump/parallel.h:55-61 and is central to pg_dump's ability to perform parallel database operations efficiently