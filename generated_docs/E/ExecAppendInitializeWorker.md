# ExecAppendInitializeWorker

## Location
src/backend/executor/nodeAppend.c: 540 - 553

## Overview
Initializes a worker process for parallel execution of an Append node by connecting to shared memory state and configuring the worker-specific subplan selection function.

## Definition
```c
void ExecAppendInitializeWorker(AppendState *node, ParallelWorkerContext *pwcxt)
```

## Detailed Description
This function sets up a worker process to participate in parallel execution of an Append node. It retrieves the shared `ParallelAppendState` structure from the table of contents (TOC) in shared memory using the plan node ID, and configures the worker to use the worker-specific subplan selection function. This allows the worker process to coordinate with other workers and the leader to efficiently distribute work across available subplans.

The function is called during the initialization phase of each worker process in parallel query execution, ensuring that workers have access to the coordination structures needed for proper work distribution.

## Parameters
- `node`: Pointer to the AppendState structure for this worker's instance of the Append node
- `pwcxt`: Pointer to the ParallelWorkerContext containing the shared memory TOC and other worker initialization data

## Dependencies
- Functions called/Symbols referenced:
  - shm_toc_lookup
  - choose_next_subplan_for_worker
- Called from (representative examples):
  - ExecParallelInitializeWorker

## Notes and Other Information
- Uses the plan node ID as the key to look up the shared state in the TOC
- Sets the worker to use `choose_next_subplan_for_worker` function, which differs from the leader's subplan selection strategy
- The `shm_toc_lookup` call uses `false` for the `noError` parameter, meaning it will raise an error if the shared state is not found
- Workers must call this function before attempting to execute any subplans to ensure proper coordination with other parallel processes
- The shared state retrieved includes synchronization primitives and work distribution information set up by the leader process