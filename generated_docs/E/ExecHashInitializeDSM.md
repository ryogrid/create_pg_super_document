# ExecHashInitializeDSM

## Location
[src/backend/executor/nodeHash.c:2760-2784](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L2760-L2784)

## Overview
ExecHashInitializeDSM sets up a shared memory space in the Dynamic Shared Memory (DSM) for all parallel workers to record instrumentation data about their hash table operations.

## Definition

```c
void
ExecHashInitializeDSM(HashState *node, ParallelContext *pcxt)
```
## Detailed Description
This function initializes shared memory infrastructure for collecting hash table instrumentation data from parallel workers. It allocates space in the DSM segment that will be used by all workers to record performance metrics and statistics about hash table operations during parallel query execution.

The function performs several key operations:
1. Checks if instrumentation is enabled and workers are present
2. Calculates the required shared memory size based on the number of workers
3. Allocates the shared memory segment using the shared memory table of contents (TOC)
4. Initializes the shared memory area to zero
5. Sets up the worker count and registers the shared info in the TOC

The shared memory layout includes a SharedHashInfo structure followed by an array of HashInstrumentation structures, one for each worker.

## Parameters / Member Variables
- `*node`: HashState pointer containing the hash node execution state and instrumentation settings
- `*pcxt`: ParallelContext pointer providing information about the parallel execution context, including the number of workers and shared memory TOC
## Dependencies
- Functions called/Symbols referenced:
  - [shm_toc_allocate](../s/shm_toc_allocate.md)
  - [shm_toc_insert](../s/shm_toc_insert.md)
  - memset (implicit)
  - offsetof (macro)
- Types used:
  - [HashState](../H/HashState.md)
  - [ParallelContext](../P/ParallelContext.md)
  - [SharedHashInfo](../S/SharedHashInfo.md)
  - [HashInstrumentation](../H/HashInstrumentation.md)
- Called from (representative examples):
  - [ExecParallelInitializeDSM](ExecParallelInitializeDSM.md)

## Notes and Other Information
- The function returns early if instrumentation is disabled (node->ps.instrument is NULL) or if there are no parallel workers (pcxt->nworkers == 0)
- The shared memory size calculation uses offsetof to account for the flexible array member in SharedHashInfo
- Each worker's instrumentation area is zero-initialized to ensure clean starting state
- The shared info is registered in the TOC using the plan node ID as the key for later lookup by workers
- This function is part of PostgreSQL's parallel query execution infrastructure, specifically for hash join operations

## Simplified Source

```c
void ExecHashInitializeDSM(HashState *node, ParallelContext *pcxt) {
    // Skip setup if no instrumentation or no workers
    if (!node->ps.instrument || pcxt->nworkers == 0)
        return;

    // Calculate size for SharedHashInfo + worker instrumentation array
    size_t size = offsetof(SharedHashInfo, hinstrument) +
                  pcxt->nworkers * sizeof(HashInstrumentation);

    // Allocate shared memory for instrumentation data
    node->shared_info = (SharedHashInfo *) shm_toc_allocate(pcxt->toc, size);

    // Initialize to zero for clean starting state
    memset(node->shared_info, 0, size);

    // Set worker count and register in shared memory TOC
    node->shared_info->num_workers = pcxt->nworkers;
    shm_toc_insert(pcxt->toc, node->ps.plan->plan_node_id, node->shared_info);
}
```