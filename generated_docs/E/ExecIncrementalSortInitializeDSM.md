# ExecIncrementalSortInitializeDSM

## Location
[src/backend/executor/nodeIncrementalSort.c:1194-1218](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIncrementalSort.c#L1194-L1218)

## Overview
Initializes dynamic shared memory (DSM) space for collecting and sharing incremental sort statistics across parallel worker processes.

## Definition


## Detailed Description
ExecIncrementalSortInitializeDSM allocates and initializes shared memory structures needed for collecting incremental sort performance statistics in parallel query execution. This function sets up the SharedIncrementalSortInfo structure that will be used to aggregate statistics from all parallel worker processes.

The function performs the following initialization steps:
1. Calculates the total size needed for the shared structure plus per-worker data
2. Allocates shared memory space using the shared memory table of contents (shm_toc)
3. Initializes the allocated memory to zero to ensure clean state
4. Sets the worker count in the shared structure
5. Registers the shared memory segment with a unique key (plan node ID) for worker access

This setup enables parallel workers to contribute their individual incremental sort statistics to a centralized location for later aggregation and reporting.

## Parameters / Member Variables
- : The IncrementalSortState that will reference the shared memory structure
- : The ParallelContext containing the shared memory table of contents and worker count

## Dependencies
- Functions called/Symbols referenced:
  - shm_toc_allocate (allocates shared memory chunk)
  - memset (initializes memory to zero)
  - shm_toc_insert (registers shared memory with unique key)
- Called from (representative examples):
  - [ExecParallelInitializeDSM](ExecParallelInitializeDSM.md) (parallel execution DSM initialization dispatcher)

## Notes and Other Information
- This function is only executed when both instrumentation is enabled and parallel workers are available
- The shared memory is keyed by the plan node ID, allowing workers to locate the correct statistics structure
- Memory is zero-initialized to ensure consistent starting state across all workers
- The SharedIncrementalSortInfo structure includes space for both the header and per-worker IncrementalSortInfo arrays
- This is part of PostgreSQL's dynamic shared memory (DSM) infrastructure for parallel query execution
- The allocated shared memory persists for the duration of the parallel query execution