# ExecSortInitializeWorker

## Location
[src/backend/executor/nodeSort.c:462-475](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSort.c#L462-L475)

## Overview
Attaches a worker process to the Dynamic Shared Memory (DSM) space for collecting and sharing tuplesort instrumentation statistics in parallel query execution.

## Definition
```c
void ExecSortInitializeWorker(SortState *node, ParallelWorkerContext *pwcxt)
```

## Detailed Description
ExecSortInitializeWorker is called during parallel worker process initialization to establish the worker's connection to the shared memory space used for tuplesort statistics collection. This function performs the worker-side setup that corresponds to the leader-side initialization done by ExecSortInitializeDSM.

The function performs two critical operations:
- Locates and attaches to the shared memory segment that was previously allocated by the leader process using the plan node ID as the lookup key
- Marks the SortState as belonging to a worker process by setting the `am_worker` flag to true

This setup allows the worker process to report its tuplesort performance statistics back to the leader process through the shared memory segment during query execution.

## Parameters / Member Variables
- `node`: Pointer to the SortState that will be configured for worker operation and linked to shared memory
- `pwcxt`: Pointer to the ParallelWorkerContext containing the shared memory TOC for looking up the shared memory segment

## Dependencies
- Functions called/Symbols referenced:
  - [shm_toc_lookup](../s/shm_toc_lookup.md) (locates shared memory segment by key)
  - [ParallelWorkerContext](../P/ParallelWorkerContext.md) (worker context structure)
- Called from (representative examples):
  - [ExecParallelInitializeWorker](ExecParallelInitializeWorker.md) (parallel worker initialization)

## Notes and Other Information
- The function uses the plan node ID from `node->ss.ps.plan->plan_node_id` as the lookup key to find the correct shared memory segment
- The `am_worker` flag is crucial for distinguishing worker behavior from leader behavior in other Sort node operations
- The shared memory lookup uses `strict = true`, meaning it will error if the shared memory segment is not found
- This function must be called after ExecSortInitializeDSM has been executed by the leader process
- The shared memory connection established here will be used later by ExecSortRetrieveInstrumentation to collect statistics

## Simplified Source

```c
void ExecSortInitializeWorker(SortState *node, ParallelWorkerContext *pwcxt) {
    // Attach to shared memory segment for sort statistics
    node->shared_info = shm_toc_lookup(pwcxt->toc, node->ss.ps.plan->plan_node_id, true);

    // Mark this as a worker process
    node->am_worker = true;
}
```