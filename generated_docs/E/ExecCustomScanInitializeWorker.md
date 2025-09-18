# ExecCustomScanInitializeWorker

## Location
[src/backend/executor/nodeCustom.c:205-220](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeCustom.c#L205-L220)

## Overview
Initializes a custom scan node in a parallel worker process by setting up access to shared coordination data.

## Definition
```c
void ExecCustomScanInitializeWorker(CustomScanState *node, ParallelWorkerContext *pwcxt)
```

## Detailed Description
ExecCustomScanInitializeWorker is responsible for initializing a custom scan node within a parallel worker process. This function locates the shared memory coordination structure that was previously set up by the leader process using ExecCustomScanInitializeDSM, and calls the custom scan provider's InitializeWorkerCustomScan method to perform worker-specific initialization. The worker can use the shared coordination data to synchronize with other workers and the leader process during parallel query execution.

## Parameters / Member Variables
- `node`: A pointer to the CustomScanState structure representing the custom scan node in the worker process
- `pwcxt`: A pointer to the ParallelWorkerContext structure containing the worker's parallel execution context and access to shared memory

## Dependencies
- Functions called/Symbols referenced:
  - [CustomScanState](../C/CustomScanState.md) (structure type)
  - [ParallelWorkerContext](../P/ParallelWorkerContext.md) (structure type)
  - [CustomExecMethods](../C/CustomExecMethods.md) (structure type)
  - [shm_toc_lookup](../s/shm_toc_lookup.md) (shared memory TOC lookup function)
- Called from (representative examples):
  - [ExecParallelInitializeWorker](ExecParallelInitializeWorker.md) (general parallel worker initializer)

## Notes and Other Information
- This function is part of PostgreSQL's parallel query execution framework and runs in worker processes
- If the custom scan provider does not implement InitializeWorkerCustomScan, no worker initialization occurs
- The shared memory coordination data must have been previously allocated by ExecCustomScanInitializeDSM in the leader process
- The plan_node_id serves as the key to locate the correct shared memory segment for this custom scan
- The coordinate pointer provides the worker access to shared state for coordination with other workers
- The shm_toc_lookup call uses 'false' for the missing_ok parameter, meaning it will error if the coordination data is not found
- This function is called once per worker process during parallel query startup