# ExecSortEstimate

## Location
[src/backend/executor/nodeSort.c:416-436](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSort.c#L416-L436)

## Overview
Estimates the shared memory space required to propagate tuplesort instrumentation statistics from worker processes to the leader process in parallel query execution.

## Definition
```c
void ExecSortEstimate(SortState *node, ParallelContext *pcxt)
```

## Detailed Description
ExecSortEstimate calculates the amount of shared memory needed to collect and share tuplesort performance statistics across parallel worker processes. This function is part of PostgreSQL's parallel query infrastructure and is called during the parallel query setup phase.

The function determines the memory requirements based on the number of worker processes and the size of instrumentation data structures. It accounts for:
- Space for TuplesortInstrumentation structures (one per worker)
- Space for the SharedSortInfo header structure
- Registration of the memory chunk and key with the shared memory table of contents

If instrumentation is disabled or no workers are involved, the function returns early without making any estimates.

## Parameters / Member Variables
- `node`: Pointer to the SortState containing the Sort node's execution state and instrumentation settings
- `pcxt`: Pointer to the ParallelContext containing information about the parallel query execution environment, including the number of workers

## Dependencies
- Functions called/Symbols referenced:
  - [mul_size](../m/mul_size.md) (safely multiplies sizes, checking for overflow)
  - [add_size](../a/add_size.md) (safely adds sizes, checking for overflow) 
  - shm_toc_estimate_chunk (estimates shared memory chunk size)
  - shm_toc_estimate_keys (estimates shared memory key requirements)
  - [TuplesortInstrumentation](../T/TuplesortInstrumentation.md) (instrumentation data structure)
  - [SharedSortInfo](../S/SharedSortInfo.md) (shared sort information structure)
- Called from (representative examples):
  - [ExecParallelEstimate](ExecParallelEstimate.md) (parallel execution estimator)

## Notes and Other Information
- Only performs estimation if instrumentation is enabled (`node->ss.ps.instrument`) and workers are present (`pcxt->nworkers > 0`)
- Uses safe arithmetic functions (mul_size, add_size) to prevent integer overflow when calculating memory requirements
- The estimated memory includes both the instrumentation data and the container structure overhead
- This estimation is critical for proper shared memory allocation in parallel sort operations