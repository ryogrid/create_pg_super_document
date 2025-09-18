# ParallelState

## Location
src/bin/pg_dump/parallel.h: 55 - 61

## Overview
ParallelState is a struct that manages the overall state for parallel backup and restore operations in pg_dump, coordinating multiple worker processes/threads to perform database dump and restore tasks concurrently.

## Definition


## Detailed Description
ParallelState serves as the central coordination structure for pg_dump's parallel processing capabilities. It maintains state information for all worker processes or threads that are executing backup or restore operations concurrently. The struct is designed to track what each worker is currently processing and provides access to the communication infrastructure needed to coordinate work distribution among the workers.

The structure supports both Unix fork()-based processes and Windows thread-based workers, providing a unified interface for parallel operations across different operating systems. It is primarily used by the leader process/thread to manage and coordinate worker activities during parallel dump and restore operations.

## Parameters / Member Variables
- : The total number of worker processes/threads allowed to run concurrently, as specified by the user
- : Array of pointers to TocEntry structures, one per worker, indicating which table of contents item each worker is currently processing (NULL if worker is idle)
- : Array of ParallelSlot structures containing private communication and state information for each worker process/thread

## Dependencies
- Functions called/Symbols referenced:
  - TocEntry
  - ParallelSlot

- Called from (representative examples):
  - ParallelBackupStart
  - ParallelBackupEnd
  - DispatchJobForTocEntry
  - GetIdleWorker
  - ShutdownWorkersHard
  - ListenToWorkers
  - WaitForWorkers
  - RestoreArchive

## Notes and Other Information
- The struct is allocated and initialized by ParallelBackupStart() when parallel operations begin
- For single-worker operations (numWorkers == 1), the te and parallelSlot arrays remain NULL as no coordination is needed
- The structure is used in both dump and restore operations, with the same coordination mechanisms applying to both scenarios
- Memory management for the arrays is handled by the parallel subsystem, with proper cleanup performed during shutdown
- The parallelSlot array contains platform-specific information (process IDs on Unix, thread handles on Windows) for communication with workers
- This struct is defined in src/bin/pg_dump/parallel.h:55-61 and is central to pg_dump's ability to perform parallel database operations efficiently