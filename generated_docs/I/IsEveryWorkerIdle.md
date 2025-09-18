# IsEveryWorkerIdle

## Location
src/bin/pg_dump/parallel.c: 1268 - 1300

## Overview
Determines whether all worker processes in the parallel operation are currently in the idle state and available for new work assignments.

## Definition
```c
bool IsEveryWorkerIdle(ParallelState *pstate)
```

## Detailed Description
This function performs a comprehensive check across all worker slots to verify that every worker process is in the WRKR_IDLE state. Unlike GetIdleWorker which finds a single idle worker, this function ensures that all workers are idle, which is important for determining when parallel operations can safely conclude or when all workers are ready for a new batch of work. It returns false immediately upon finding any worker that is not idle, using an early termination optimization for efficiency.

## Parameters / Member Variables
- `pstate`: Pointer to the ParallelState structure containing the array of worker slots and their current status information

## Dependencies
- Functions called/Symbols referenced:
  - ParallelState (struct)
  - WRKR_IDLE (constant)
- Called from (representative examples):
  - ParallelBackupEnd
  - WaitForWorkers
  - restore_toc_entries_parallel

## Notes and Other Information
- Returns true only when ALL workers are in the WRKR_IDLE state, false otherwise
- Uses early termination - returns false as soon as a non-idle worker is found
- This function has public visibility (not static) unlike some other worker management functions
- Critical for synchronization points in parallel operations where all workers must be idle before proceeding
- Commonly used at the end of parallel phases to ensure completion before cleanup or next phase
- More restrictive than HasEveryWorkerTerminated, as it specifically requires the idle state rather than just non-running