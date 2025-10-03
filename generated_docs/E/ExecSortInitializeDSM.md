# ExecSortInitializeDSM

## Location
[src/backend/executor/nodeSort.c:437-461](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSort.c#L437-L461)

## Overview
Initializes Dynamic Shared Memory (DSM) space for collecting and sharing tuplesort instrumentation statistics across parallel worker processes.

## Definition
```c
void ExecSortInitializeDSM(SortState *node, ParallelContext *pcxt)
```

## Detailed Description
ExecSortInitializeDSM allocates and initializes shared memory space that will be used to collect tuplesort performance statistics from parallel worker processes. This function is called during the parallel query setup phase, after the memory requirements have been estimated by ExecSortEstimate.

The function performs several key operations:
- Calculates the exact memory size needed for the SharedSortInfo structure and worker instrumentation data
- Allocates the shared memory space using the shared memory table of contents (TOC)
- Initializes the allocated memory to zero to ensure clean state
- Sets up the SharedSortInfo structure with the number of workers
- Registers the shared memory segment with the TOC using the plan node ID as the key

This shared memory will later be used by worker processes to report their tuplesort statistics back to the leader process.

## Parameters / Member Variables
- `node`: Pointer to the SortState that will store the reference to the allocated shared memory
- `pcxt`: Pointer to the ParallelContext containing the shared memory TOC and worker count information

## Dependencies
- Functions called/Symbols referenced:
  - [shm_toc_allocate](../s/shm_toc_allocate.md) (allocates shared memory from TOC)
  - [shm_toc_insert](../s/shm_toc_insert.md) (registers shared memory segment in TOC)
  - memset (initializes memory to zero)
  - [SharedSortInfo](../S/SharedSortInfo.md) (shared sort information structure)
  - [TuplesortInstrumentation](../T/TuplesortInstrumentation.md) (instrumentation data structure)
- Called from (representative examples):
  - [ExecParallelInitializeDSM](ExecParallelInitializeDSM.md) (parallel execution DSM initializer)

## Notes and Other Information
- Only performs initialization if instrumentation is enabled and workers are present
- The allocated memory is zero-initialized to ensure deterministic behavior for unused slots
- Uses the plan node ID as the key for TOC registration, allowing workers to locate the shared memory
- The shared_info pointer in the SortState provides access to the allocated memory throughout execution
- Memory layout includes the SharedSortInfo header followed by an array of TuplesortInstrumentation structures

## Simplified Source

```c
void ExecSortInitializeDSM(SortState *node, ParallelContext *pcxt) {
    // Skip if no instrumentation or no workers
    if (!node->ss.ps.instrument || pcxt->nworkers == 0)
        return;

    // Calculate size: header + per-worker instrumentation array
    Size size = offsetof(SharedSortInfo, sinstrument) +
                pcxt->nworkers * sizeof(TuplesortInstrumentation);

    // Allocate shared memory for statistics collection
    node->shared_info = shm_toc_allocate(pcxt->toc, size);

    // Initialize to zero for clean starting state
    memset(node->shared_info, 0, size);

    // Set worker count and register in shared memory TOC
    node->shared_info->num_workers = pcxt->nworkers;
    shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id, node->shared_info);
}
```