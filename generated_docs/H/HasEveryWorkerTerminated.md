# HasEveryWorkerTerminated

## Location
[src/bin/pg_dump/parallel.c:1252-1267](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/parallel.c#L1252-L1267)

## Overview
Checks whether all worker processes in the parallel operation have terminated by examining the status of each worker slot.

## Definition
```c
static bool HasEveryWorkerTerminated(ParallelState *pstate)
```

## Detailed Description
This function iterates through all worker slots in the parallel state to determine if every worker process has finished running. It uses the WORKER_IS_RUNNING macro to check each worker's status and returns false immediately if any worker is still active. The function returns true only when all workers have terminated, making it useful for determining when parallel operations have completely finished and it's safe to proceed with cleanup or final processing steps.

## Parameters / Member Variables
- `pstate`: Pointer to the ParallelState structure containing information about all worker processes and their current states

## Dependencies
- Functions called/Symbols referenced:
  - [ParallelState](../P/ParallelState.md) (struct)
  - WORKER_IS_RUNNING (macro)
- Called from (representative examples):
  - [WaitForTerminatingWorkers](../W/WaitForTerminatingWorkers.md)
  - [write_stderr](../w/write_stderr.md)

## Notes and Other Information
- Returns true if all workers have terminated, false if any worker is still running
- Uses early termination optimization - returns false as soon as a running worker is found
- The function is static and only used within the parallel.c module
- Essential for coordinating the shutdown phase of parallel operations
- Relies on the WORKER_IS_RUNNING macro which likely checks for specific status values indicating active worker states
- Used primarily during cleanup and termination sequences to ensure all workers have finished before proceeding

## Simplified Source

```c
static bool HasEveryWorkerTerminated(ParallelState *pstate) {
    // Check all workers to see if any are still running
    for (int i = 0; i < pstate->numWorkers; i++) {
        if (WORKER_IS_RUNNING(pstate->parallelSlot[i].workerStatus)) {
            return false;  // Found a running worker
        }
    }
    return true;  // All workers have terminated
}
```