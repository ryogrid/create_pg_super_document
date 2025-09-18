# GetIdleWorker

## Location
src/bin/pg_dump/parallel.c: 1236 - 1251

## Overview
Searches through the worker pool to find an available worker process and returns its slot number, or indicates if no workers are currently idle.

## Definition
```c
static int GetIdleWorker(ParallelState *pstate)
```

## Detailed Description
This function performs a simple linear search through the parallel worker slots to locate a worker that is currently in the idle state (WRKR_IDLE). It examines each worker slot sequentially and returns the slot number of the first idle worker found. If no workers are available (all are busy or terminated), it returns NO_SLOT to indicate that no idle worker is available. This function is fundamental to the job dispatching mechanism in parallel pg_dump operations.

## Parameters / Member Variables
- `pstate`: Pointer to the ParallelState structure containing the array of worker slots and their current states

## Dependencies
- Functions called/Symbols referenced:
  - [ParallelState](../P/ParallelState.md) (struct)
  - WRKR_IDLE (constant)
  - NO_SLOT (constant)
- Called from (representative examples):
  - [DispatchJobForTocEntry](../D/DispatchJobForTocEntry.md)
  - [WaitForWorkers](../W/WaitForWorkers.md)
  - [write_stderr](../w/write_stderr.md)

## Notes and Other Information
- Returns the first available worker slot number (0-based index) or NO_SLOT if none available
- Uses a simple linear search algorithm which is efficient for the typical small number of worker processes
- The function is static and only used within the parallel.c module
- Critical for resource management in parallel operations to avoid over-scheduling work
- The returned slot number can be used directly to access worker-specific information in the parallelSlot array