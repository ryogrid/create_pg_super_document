# ExecAggEstimate

## Location
[src/backend/executor/nodeAgg.c:4683-4703](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L4683-L4703)

## Overview
Estimates the shared memory space required to propagate aggregate execution statistics in parallel query execution.

## Definition
```c
void ExecAggEstimate(AggState *node, ParallelContext *pcxt)
```

## Detailed Description
ExecAggEstimate is part of PostgreSQL's parallel query support infrastructure. It calculates the amount of shared memory needed to store and communicate aggregate execution instrumentation data between parallel workers and the leader process.

The function performs a space estimation for the SharedAggInfo structure, which includes instrumentation data for each parallel worker. The calculation accounts for the number of workers and the size of AggregateInstrumentation data that needs to be shared. This estimation is used by the parallel query planner to allocate appropriate shared memory segments.

The function includes an early exit optimization: if instrumentation is not enabled or there are no parallel workers, no shared memory is needed for aggregate statistics.

## Parameters / Member Variables
- `node`: AggState pointer containing the aggregate execution state and instrumentation settings
- `pcxt`: ParallelContext pointer containing information about the parallel execution context, including the number of workers

## Dependencies
- Functions called/Symbols referenced:
  - [mul_size](../m/mul_size.md) (safe multiplication for size calculations)
  - [add_size](../a/add_size.md) (safe addition for size calculations)
  - shm_toc_estimate_chunk (estimates shared memory chunk size)
  - shm_toc_estimate_keys (estimates shared memory key count)
  - [AggregateInstrumentation](../A/AggregateInstrumentation.md) (structure for aggregate execution statistics)
  - [SharedAggInfo](../S/SharedAggInfo.md) (shared aggregate information structure)
- Called from (representative examples):
  - ExecParallelEstimate (in execParallel.c)

## Notes and Other Information
- Only performs estimation when both instrumentation is enabled and parallel workers exist
- The size calculation includes space for AggregateInstrumentation data for each worker
- Uses PostgreSQL's safe arithmetic functions (mul_size, add_size) to prevent overflow
- Part of the parallel query infrastructure for sharing execution statistics
- The estimated space is registered with the shared memory table of contents (TOC)
- Estimates exactly one key in the shared memory TOC for the aggregate instrumentation data
- Returns early without estimation if instrumentation is disabled or no workers are configured