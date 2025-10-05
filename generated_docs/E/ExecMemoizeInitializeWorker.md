# ExecMemoizeInitializeWorker

## Location
[src/backend/executor/nodeMemoize.c:1236-1248](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMemoize.c#L1236-L1248)

## Overview
Attaches a parallel worker process to the previously initialized Dynamic Shared Memory (DSM) space for memoize statistics collection.

## Definition
```c
void ExecMemoizeInitializeWorker(MemoizeState *node, ParallelWorkerContext *pwcxt)
```

## Detailed Description
ExecMemoizeInitializeWorker is called by each parallel worker process to establish a connection to the shared memory segment containing memoize execution statistics. This function looks up the SharedMemoizeInfo structure that was previously allocated and registered by the leader process during DSM initialization. The function is essential for enabling workers to contribute their instrumentation data to the shared statistics collection.

The function performs a simple lookup in the shared memory table of contents (TOC) using the plan node ID as the key, then stores the reference in the worker's MemoizeState structure for later use during execution and statistics reporting.

## Parameters / Member Variables
- `node`: Pointer to the worker's MemoizeState structure where the shared memory reference will be stored
- `pwcxt`: Pointer to the ParallelWorkerContext structure containing the worker's shared memory TOC access

## Dependencies
- Functions called/Symbols referenced:
  - [shm_toc_lookup](../s/shm_toc_lookup.md) (looks up shared memory segment by key in the TOC)
- Types referenced:
  - [MemoizeState](../M/MemoizeState.md) (memoize execution state structure)
  - [ParallelWorkerContext](../P/ParallelWorkerContext.md) (parallel worker execution context)
- Called from:
  - [ExecParallelInitializeWorker](ExecParallelInitializeWorker.md) (main worker initialization function)

## Notes and Other Information
- This function is called by each worker process during parallel execution setup
- The shared memory segment must have been previously allocated by ExecMemoizeInitializeDSM in the leader process
- Uses the plan node ID as the lookup key to find the correct SharedMemoizeInfo structure
- The third parameter (true) to shm_toc_lookup indicates that the lookup must succeed (will error if not found)
- Once initialized, workers can use the shared_info pointer to update their instrumentation data during execution
- This is a lightweight operation that simply establishes the connection to shared memory without allocating new memory

## Simplified Source

```c
void
ExecMemoizeInitializeWorker(MemoizeState *node, ParallelWorkerContext *pwcxt)
{
    // Connect worker to shared memory for memoize statistics
    // Lookup the shared info structure using the plan node ID
    node->shared_info =
        shm_toc_lookup(pwcxt->toc, node->ss.ps.plan->plan_node_id, true);
}
```