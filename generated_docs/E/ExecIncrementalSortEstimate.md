# ExecIncrementalSortEstimate

## Location
[src/backend/executor/nodeIncrementalSort.c:1173-1193](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIncrementalSort.c#L1173-L1193)

## Overview
Estimates the shared memory space required to propagate incremental sort statistics and instrumentation data across parallel worker processes.

## Definition


## Detailed Description
ExecIncrementalSortEstimate calculates the amount of shared memory needed to collect and share incremental sort performance statistics across parallel worker processes. This function is part of PostgreSQL's parallel query execution infrastructure and is called during the parallel query planning phase.

The function calculates the required shared memory size by:
1. Determining space needed for IncrementalSortInfo structures (one per worker)
2. Adding space for the SharedIncrementalSortInfo header structure
3. Registering the memory requirements with the shared memory table of contents (shm_toc)

The estimation is only performed when both instrumentation is enabled and parallel workers are available, as the statistics collection is unnecessary without these conditions.

## Parameters / Member Variables
- : The IncrementalSortState containing instrumentation settings and configuration
- : The ParallelContext containing worker count and shared memory estimator

## Dependencies
- Functions called/Symbols referenced:
  - [mul_size](../m/mul_size.md) (safely multiplies sizes with overflow checking)
  - [add_size](../a/add_size.md) (safely adds sizes with overflow checking)  
  - shm_toc_estimate_chunk (estimates shared memory chunk space)
  - shm_toc_estimate_keys (estimates shared memory key space)
- Called from (representative examples):
  - ExecParallelEstimate (parallel execution space estimation dispatcher)

## Notes and Other Information
- This function is part of PostgreSQL's parallel query support infrastructure
- Memory estimation is only performed when instrumentation is enabled and workers are available
- The calculation accounts for both per-worker IncrementalSortInfo structures and the shared header
- Uses PostgreSQL's safe arithmetic functions (mul_size, add_size) to prevent integer overflow
- The shared memory table of contents (shm_toc) tracks both chunk space and key space requirements
- Statistics collected include metrics for both fullsort and prefixsort operations across parallel workers