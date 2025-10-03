# ExecAggInitializeDSM

## Location
[src/backend/executor/nodeAgg.c:4704-4728](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L4704-L4728)

## Overview
Initializes Dynamic Shared Memory (DSM) space for storing aggregate execution statistics in parallel query processing.

## Definition
```c
void ExecAggInitializeDSM(AggState *node, ParallelContext *pcxt)
```

## Detailed Description
ExecAggInitializeDSM is responsible for setting up the shared memory infrastructure needed to collect and share aggregate execution instrumentation data across parallel workers. This function allocates and initializes a SharedAggInfo structure in the Dynamic Shared Memory segment, which will be used to store performance statistics from all parallel workers executing the aggregate operation.

The function calculates the required memory size based on the number of parallel workers and allocates space for both the SharedAggInfo header and an array of AggregateInstrumentation structures (one per worker). The allocated memory is zeroed to ensure consistent initial state, and the structure is registered in the shared memory table of contents using the plan node ID as the key.

## Parameters / Member Variables
- `node`: AggState pointer representing the aggregate execution state, which will store the reference to shared instrumentation data
- `pcxt`: ParallelContext pointer containing the parallel execution context, including worker count and shared memory table of contents

## Dependencies
- Functions called/Symbols referenced:
  - [shm_toc_allocate](../s/shm_toc_allocate.md) (allocates space in shared memory table of contents)
  - [shm_toc_insert](../s/shm_toc_insert.md) (inserts entry into shared memory table of contents)
  - memset (zeros the allocated memory)
  - [SharedAggInfo](../S/SharedAggInfo.md) (shared aggregate information structure)
  - [AggregateInstrumentation](../A/AggregateInstrumentation.md) (per-worker instrumentation structure)
- Called from (representative examples):
  - [ExecParallelInitializeDSM](ExecParallelInitializeDSM.md) (in execParallel.c)

## Notes and Other Information
- Only performs initialization when both instrumentation is enabled and parallel workers exist
- Allocates memory for SharedAggInfo header plus one AggregateInstrumentation per worker
- The allocated memory is zero-initialized to ensure consistent starting state
- Sets the num_workers field in the shared structure to track the worker count
- Uses the plan node ID as the key for shared memory table of contents lookup
- The shared_info pointer in the AggState node references the allocated shared memory
- Part of PostgreSQL's parallel execution infrastructure for performance monitoring
- Works in conjunction with ExecAggEstimate to properly size the shared memory allocation

## Simplified Source

```c
void ExecAggInitializeDSM(AggState *node, ParallelContext *pcxt)
{
    Size size;

    // Skip initialization if no instrumentation or workers
    if (!node->ss.ps.instrument || pcxt->nworkers == 0)
        return;

    // Calculate total memory needed for shared aggregate info
    size = offsetof(SharedAggInfo, sinstrument) +
           pcxt->nworkers * sizeof(AggregateInstrumentation);

    // Allocate and initialize shared memory
    node->shared_info = shm_toc_allocate(pcxt->toc, size);
    memset(node->shared_info, 0, size);
    node->shared_info->num_workers = pcxt->nworkers;

    // Register in shared memory table of contents
    shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id, node->shared_info);
}
```