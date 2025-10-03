# ExecMemoizeEstimate

## Location
[src/backend/executor/nodeMemoize.c:1190-1210](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMemoize.c#L1190-L1210)

## Overview
Estimates the shared memory space required to propagate memoize execution statistics across parallel worker processes.

## Definition

```c
void
ExecMemoizeEstimate(MemoizeState *node, ParallelContext *pcxt)
```
## Detailed Description
ExecMemoizeEstimate is a parallel query support function that calculates the shared memory requirements for propagating memoize execution statistics from worker processes back to the leader process. The function is only relevant when instrumentation is enabled and parallel workers are being used. It estimates the memory needed to store MemoizeInstrumentation data for each worker process in a shared memory structure (SharedMemoizeInfo).

The function performs memory size calculations using PostgreSQL's safe arithmetic functions to prevent overflow, then registers the estimated chunk size and number of keys with the shared memory table of contents (TOC) estimator.

## Parameters / Member Variables
- `*node`: Pointer to the MemoizeState structure containing the memoize execution state and instrumentation settings
- `*pcxt`: Pointer to the ParallelContext structure containing parallel execution context including the number of workers
## Dependencies
- Functions called/Symbols referenced:
  - [mul_size](../m/mul_size.md) (safe multiplication for memory size calculations)
  - [add_size](../a/add_size.md) (safe addition for memory size calculations) 
  - shm_toc_estimate_chunk (estimates shared memory chunk requirements)
  - shm_toc_estimate_keys (estimates shared memory key requirements)
- Types referenced:
  - [MemoizeState](../M/MemoizeState.md) (memoize execution state structure)
  - [ParallelContext](../P/ParallelContext.md) (parallel execution context)
  - [MemoizeInstrumentation](../M/MemoizeInstrumentation.md) (memoize statistics structure)
  - [SharedMemoizeInfo](../S/SharedMemoizeInfo.md) (shared memory info structure)
- Called from:
  - [ExecParallelEstimate](ExecParallelEstimate.md) (main parallel execution estimator)

## Notes and Other Information
- Only performs estimation when instrumentation is enabled (node->ss.ps.instrument is true) and parallel workers are present (pcxt->nworkers > 0)
- The estimated memory size includes space for one MemoizeInstrumentation structure per worker plus the SharedMemoizeInfo header
- Uses PostgreSQL's safe arithmetic functions (mul_size, add_size) to prevent integer overflow in size calculations
- Part of PostgreSQL's parallel query execution framework for memoize operations

## Simplified Source

```c
void ExecMemoizeEstimate(MemoizeState *node, ParallelContext *pcxt)
{
    Size size;

    // Skip estimation if no instrumentation or workers
    if (!node->ss.ps.instrument || pcxt->nworkers == 0)
        return;

    // Calculate memory needed: worker count * instrumentation size + shared header
    size = mul_size(pcxt->nworkers, sizeof(MemoizeInstrumentation));
    size = add_size(size, offsetof(SharedMemoizeInfo, sinstrument));

    // Register memory requirements with shared memory allocator
    shm_toc_estimate_chunk(&pcxt->estimator, size);
    shm_toc_estimate_keys(&pcxt->estimator, 1);
}
```