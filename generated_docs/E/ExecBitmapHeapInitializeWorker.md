# ExecBitmapHeapInitializeWorker

## Location
[src/backend/executor/nodeBitmapHeapscan.c:894-903](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeBitmapHeapscan.c#L894-L903)

## Overview
This function initializes a worker process for parallel bitmap heap scan by connecting it to the shared state created by the leader process.

## Definition
```c
void ExecBitmapHeapInitializeWorker(BitmapHeapScanState *node, ParallelWorkerContext *pwcxt)
```

## Detailed Description
ExecBitmapHeapInitializeWorker is called in each worker process during parallel query initialization to establish access to the shared memory state for bitmap heap scanning. The function performs a lookup in the shared memory Table of Contents (TOC) to find the ParallelBitmapHeapState structure that was previously allocated and initialized by the leader process via ExecBitmapHeapInitializeDSM.

The function is quite simple but critical for parallel execution:
1. It uses the plan node ID as a key to lookup the shared state in the TOC
2. Connects the worker's local BitmapHeapScanState to the shared ParallelBitmapHeapState
3. Includes an assertion to ensure DSA (Dynamic Shared Area) is available, indicating parallel execution is properly set up

After this initialization, the worker process can participate in coordinated bitmap heap scanning using the shared synchronization primitives and state.

## Parameters / Member Variables
- `node`: Pointer to BitmapHeapScanState for this worker process, which will be connected to shared state
- `pwcxt`: Pointer to ParallelWorkerContext containing the shared memory TOC and other parallel execution context

## Dependencies
- Functions called/Symbols referenced:
  - [shm_toc_lookup](../s/shm_toc_lookup.md)
  - Assert (macro)
  - [BitmapHeapScanState](../B/BitmapHeapScanState.md) (structure)
  - [ParallelWorkerContext](../P/ParallelWorkerContext.md) (structure)
  - [ParallelBitmapHeapState](../P/ParallelBitmapHeapState.md) (structure)
- Called from (representative examples):
  - [ExecParallelInitializeWorker](ExecParallelInitializeWorker.md)

## Notes and Other Information
This function is the counterpart to ExecBitmapHeapInitializeDSM - while the leader process creates and initializes the shared state, worker processes use this function to connect to that pre-existing shared state. The function includes a critical assertion that verifies DSA availability, as this is required for all shared memory operations. The plan_node_id serves as a unique identifier ensuring each worker connects to the correct shared state when multiple parallel bitmap scans exist in the same query.

## Simplified Source

```c
void ExecBitmapHeapInitializeWorker(BitmapHeapScanState *node,
                                   ParallelWorkerContext *pwcxt) {
    // Verify parallel execution is properly set up
    Assert(node->ss.ps.state->es_query_dsa != NULL);

    // Connect worker to shared bitmap heap scan state
    ParallelBitmapHeapState *pstate = shm_toc_lookup(pwcxt->toc,
                                                      node->ss.ps.plan->plan_node_id,
                                                      false);
    node->pstate = pstate;
}
```